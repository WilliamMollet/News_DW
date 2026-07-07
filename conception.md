# Document de conception — Pipeline Lakehouse Noozra

> Livrable partie 3 du TP autonome. Les diagrammes sont en Mermaid et se rendent
> directement sur GitHub/GitLab.

---

## 1. Formulation écrite du problème métier

**Domaine métier.** Veille média : suivi du flux d'articles publiés par un
agrégateur d'actualités multi-sources (API publique Noozra), couvrant plusieurs
rubriques (world, sports, tech, business, entertainment, general…).

**Question métier.** *Quelles catégories et quelles sources dominent le flux
d'actualité sur les 7 derniers jours, et à quelles heures de la journée le
volume de publication est-il le plus dense ?* La réponse permet une décision
concrète : choisir l'heure d'envoi de la newsletter quotidienne et rééquilibrer
la couverture éditoriale par rubrique.

**Destinataire.** La responsable de veille éditoriale d'une rédaction. Elle
consulte le dashboard chaque matin pour (1) caler l'heure d'envoi de sa
newsletter sur les pics de publication, (2) repérer les rubriques
sur- ou sous-alimentées, (3) identifier les sources les plus productives à
surveiller en priorité.

**Lien avec le dashboard (partie 6).** Chaque visualisation se justifie par
rapport à cette question :

| Visualisation | Partie de la question couverte |
|---|---|
| Courbe du nombre d'articles par jour (7 j) | tendance du volume global |
| Barres empilées articles par catégorie / par source | qui domine le flux |
| Heatmap ou barres du volume par heure de la journée | meilleur créneau newsletter |
| KPI : total articles 7 j + % avec image | indicateur agrégé clé |

---

## 2. Schéma d'architecture data

```mermaid
flowchart LR
    subgraph EXT["Source externe"]
        API["API Noozra<br/>https://noozra.com/api<br/>JSON, sans clé, 100 req/j/IP"]
    end

    subgraph ORCH["Orchestration — Airflow 2.10 (LocalExecutor)"]
        DAG["DAG news_pipeline<br/>TaskFlow API"]
        META[("postgres-airflow<br/>metadata DB dédiée")]
        DAG -.-> META
    end

    subgraph LAKE["Lakehouse"]
        subgraph MINIO["MinIO (object storage)"]
            BRONZE["Zone BRONZE<br/>JSON brut horodaté<br/>bucket : bronze"]
        end
        subgraph PGDW["PostgreSQL DW — base newsdw"]
            SILVER["Zone SILVER<br/>schéma silver<br/>données nettoyées + typées"]
            QUAR["QUARANTAINE<br/>schéma quarantine<br/>lignes rejetées + motif"]
            GOLD["Zone GOLD<br/>schéma gold<br/>modèle en étoile"]
            LOGS["schéma logs<br/>technical_log + quality_log"]
        end
    end

    subgraph BI["Restitution"]
        MB["Metabase<br/>dashboard métier"]
    end

    API -->|"extract (1 appel / catégorie,<br/>en parallèle)"| DAG
    DAG -->|"JSON brut, aucune transformation"| BRONZE
    BRONZE -->|"nettoyage + 5 règles qualité"| SILVER
    BRONZE -->|"lignes invalides"| QUAR
    SILVER -->|"chargement idempotent<br/>(upsert sur article_id)"| GOLD
    DAG -.->|"logs métier"| LOGS
    GOLD --> MB
```

**Choix structurants**

- **Bronze sur MinIO** : le JSON de l'API est stocké tel quel
  (`bronze/noozra/dt=YYYY-MM-DD/run=<run_id>/<categorie>.json`), ce qui rend
  tout retraitement possible sans rappeler l'API (précieux avec le quota de
  100 req/jour).
- **Silver et gold dans PostgreSQL** : données typées, contraintes SQL,
  requêtes d'idempotence simples, et branchement direct de Metabase.
- **Metadata DB Airflow séparée** (`postgres-airflow`) : exigée par l'énoncé,
  et nécessaire au LocalExecutor pour exécuter les extractions par catégorie
  en parallèle.

---

## 3. Diagramme du pipeline (DAG `news_pipeline`)

```mermaid
flowchart TD
    START(["Déclenchement<br/>@hourly (ou manuel)"]) --> EXTG

    subgraph EXTG["extract — tâches parallèles (LocalExecutor)"]
        E1["extract_world<br/>GET /articles?category=world&limit=100"]
        E2["extract_sports<br/>GET /articles?category=sports&limit=100"]
        E3["extract_tech<br/>GET /articles?category=tech&limit=100"]
        E4["extract_business<br/>GET /articles?category=business&limit=100"]
    end

    EXTG -->|"retries=2, retry_delay=30s,<br/>execution_timeout=60s"| LB["load_bronze<br/>écrit le JSON brut dans MinIO"]

    LB --> QC["quality_checks<br/>5 règles sur le batch bronze :<br/>complétude · exactitude · cohérence ·<br/>fraîcheur · unicité<br/>→ résultats loggés dans logs.quality_log"]

    QC -->|"lignes valides"| TS["transform_silver<br/>typage, normalisation,<br/>dédoublonnage sur id"]
    QC -->|"lignes invalides<br/>(+ motif de rejet)"| QU["quarantine<br/>insert dans quarantine.articles_rejetes<br/>⚠ le DAG reste VERT"]

    TS --> LG["load_gold<br/>upsert dims puis fact<br/>ON CONFLICT (article_id) DO NOTHING<br/>→ idempotent"]

    QU --> NOTIF["log_quality_report<br/>bilan qualité du run"]
    LG --> NOTIF
    NOTIF --> END(["Fin de run"])

    style QU fill:#fff3cd,stroke:#b8860b
    style QC fill:#e7f1ff,stroke:#1c64c8
```

**Les trois chemins démontrables (livrable 3)**

| Chemin | Comment le déclencher | Résultat attendu dans l'UI Airflow |
|---|---|---|
| **Nominal** | run normal | DAG vert, lignes en silver + gold, `quality_log.passed = true` |
| **Échec qualité** | Variable Airflow `news_inject_bad_data = true` → injection dans le bronze d'articles corrompus (`headline` manquant, `category` inconnue, `published_at` futur) | DAG **vert**, lignes déviées en `quarantine.articles_rejetes`, `quality_log.records_failed > 0` |
| **Échec technique** | Variable Airflow `noozra_base_url = https://noozra.com/api-inexistant` | tâches extract en échec après 2 retries → DAG **rouge**, erreur dans `logs.technical_log` |

**Paramètres de robustesse** (sur toutes les tâches faisant de l'I/O réseau ou
DB) : `retries=2`, `retry_delay=timedelta(seconds=30)`,
`execution_timeout=timedelta(seconds=60)`. Le plafond de 2 retries est
volontaire : quota API de 100 requêtes/jour/IP.

**Logging à deux niveaux** : logs Airflow natifs par tâche (visibles dans
l'UI) **et** logs métier persistés en base — `logs.quality_log` (une ligne par
règle et par run) et `logs.technical_log` (erreurs applicatives, volumétries).

---

## 4. Modèle de données gold — schéma en étoile

```mermaid
erDiagram
    DIM_DATE {
        int      date_sk PK "format AAAAMMJJ"
        date     date_complete
        int      annee
        int      mois
        int      jour
        text     nom_jour_semaine
        int      numero_semaine_iso
        boolean  est_weekend
    }

    DIM_HEURE {
        int   heure_sk PK "0 à 23"
        text  tranche_horaire "nuit / matin / apres-midi / soiree"
    }

    DIM_SOURCE {
        int   source_sk PK "surrogate key (serial)"
        text  source_name UK "ex. BBC World News"
    }

    DIM_CATEGORY {
        int   category_sk PK "surrogate key (serial)"
        text  category_slug UK "ex. tech — validé contre /api/categories"
    }

    FACT_ARTICLE {
        uuid        article_id PK "clé métier Noozra — support de l'idempotence"
        int         date_sk FK
        int         heure_sk FK
        int         source_sk FK
        int         category_sk FK
        text        headline "dimension dégénérée"
        text        url "dimension dégénérée"
        boolean     has_image "mesure"
        int         description_length "mesure (caractères)"
        int         image_width "mesure, nullable"
        int         image_height "mesure, nullable"
        timestamptz published_at
        timestamptz loaded_at
        text        run_id "traçabilité du run Airflow"
    }

    DIM_DATE     ||--o{ FACT_ARTICLE : "date de publication"
    DIM_HEURE    ||--o{ FACT_ARTICLE : "heure de publication"
    DIM_SOURCE   ||--o{ FACT_ARTICLE : "publié par"
    DIM_CATEGORY ||--o{ FACT_ARTICLE : "classé en"
```

**Grain de la table de faits** : 1 ligne = 1 article publié. La mesure
principale est le comptage (`COUNT(*)`), enrichie de mesures descriptives
(`has_image`, `description_length`).

**Idempotence du chargement gold** : clé primaire sur `article_id` (UUID fourni
par l'API) et chargement en `INSERT … ON CONFLICT (article_id) DO NOTHING`.
Rejouer le même run ne crée aucun doublon. Vérification (livrable 4) :

```sql
-- Doit retourner 0 ligne, avant comme après un re-run du DAG
SELECT article_id, COUNT(*)
FROM gold.fact_article
GROUP BY article_id
HAVING COUNT(*) > 1;

-- Volumétrie stable entre deux exécutions identiques
SELECT COUNT(*) AS nb_faits FROM gold.fact_article;
```

**Correspondance question métier ↔ modèle** : le volume par jour s'obtient via
`dim_date`, le créneau horaire via `dim_heure`, la domination par rubrique et
par source via `dim_category` et `dim_source` — chaque axe de la question a sa
dimension dédiée.

---

## 5. Règles qualité (bronze → silver)

| # | Dimension qualité | Règle appliquée | Si échec |
|---|---|---|---|
| 1 | **Complétude** | `id`, `headline`, `url`, `published_at`, `source`, `category` non nuls et non vides | quarantaine |
| 2 | **Exactitude** | `url` commence par `http(s)://` ; `published_at` parseable ISO 8601 ; `image_width/height` > 0 ou null | quarantaine |
| 3 | **Cohérence** | `category` ∈ référentiel `GET /api/categories` ; `published_at` ≤ horodatage du run | quarantaine |
| 4 | **Fraîcheur** | l'article le plus récent du batch a moins de 24 h | warning loggé (règle de batch, pas de ligne) |
| 5 | **Unicité** | `id` unique dans le batch et absent de silver (dédoublonnage) | doublon écarté et loggé |

Chaque règle est appliquée **et loggée séparément** dans `logs.quality_log`
(une ligne par règle et par run : `records_checked`, `records_failed`,
`passed`, `details` JSONB).

---

## 6. Conventions de nommage

- **MinIO** : `bronze/noozra/dt=<YYYY-MM-DD>/run=<run_id>/<categorie>.json`
- **Schémas Postgres** : `silver`, `quarantine`, `gold`, `logs` (anglais, singulier)
- **Tables gold** : préfixes `dim_` / `fact_` ; clés de substitution suffixées `_sk`
- **Tâches Airflow** : `verbe_objet` en snake_case (`extract_tech`, `load_bronze`, `quality_checks`, `transform_silver`, `load_gold`)
- **DAG** : `news_pipeline` ; `run_id` Airflow propagé jusqu'à `fact_article.run_id` pour la traçabilité de bout en bout