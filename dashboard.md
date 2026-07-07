# Dashboard Metabase — « Pilotage du flux d'actualité »

> Livrable 5. Chaque visualisation est justifiée par rapport à la question
> métier (partie 1) : *quelles catégories et sources dominent le flux sur les
> 7 derniers jours, et à quelles heures le volume de publication est-il le
> plus dense ?*

---

## 0. Connexion de Metabase au DW (une seule fois)

1. Ouvrir http://localhost:3000 et terminer le setup initial.
2. Ajouter une base de données : **PostgreSQL**
   - Nom : `News DW` — Host : `postgres-dw` — Port : `5432`
   - Base : `newsdw` — Utilisateur : `dw` — Mot de passe : `dw`
3. Chaque visualisation ci-dessous = **+ Nouveau > Question SQL native** sur
   `News DW`, puis « Enregistrer » et « Ajouter au dashboard ».
4. Créer le dashboard **« Pilotage du flux d'actualité — 7 derniers jours »**
   et y épingler les 4 questions.

Toutes les requêtes interrogent **exclusivement la zone gold** (schéma en
étoile), jamais silver — c'est le rôle de la couche de restitution.

---

## 1. KPI — Volume total et complétude visuelle du flux

**Justification** : l'« indicateur agrégé clé » demandé par l'énoncé ; donne
l'ordre de grandeur du flux que la responsable de veille doit couvrir.

**Type de visualisation** : Number (deux cartes Number côte à côte, un pour articles_sur_7_jours en style normal et un pour pct_avec_image en style pourcentage avec 0.01 en coefficient multiplicateur).

**Titre dans Metabase** : `Articles publiés sur 7 jours (et % avec image)`

```sql
SELECT
    COUNT(*)                                                   AS articles_sur_7_jours,
    ROUND(100.0 * AVG(CASE WHEN f.has_image THEN 1 ELSE 0 END), 1)
                                                               AS pct_avec_image
FROM gold.fact_article f
JOIN gold.dim_date d ON d.date_sk = f.date_sk
WHERE d.date_complete >= CURRENT_DATE - 6;
```

---

## 2. Évolution — Volume d'articles par jour

**Justification** : la partie « tendance » de la question — le flux
grossit-il, se tarit-il, y a-t-il un creux le week-end ?

**Type** : Line (X = `jour`, Y = `nb_articles`).

**Titre** : `Évolution quotidienne du volume d'articles (7 derniers jours)`

```sql
SELECT
    d.date_complete       AS jour,
    d.nom_jour_semaine    AS jour_semaine,
    COUNT(*)              AS nb_articles
FROM gold.fact_article f
JOIN gold.dim_date d ON d.date_sk = f.date_sk
WHERE d.date_complete >= CURRENT_DATE - 6
GROUP BY d.date_complete, d.nom_jour_semaine
ORDER BY d.date_complete;
```

---

## 3. Comparaison — Qui domine le flux : catégories × sources

**Justification** : la partie « quelles catégories et quelles sources
dominent » — c'est la visualisation qui déclenche la décision de
rééquilibrage éditorial.

**Type** : Camembert (Anneau extérieur = `categorie`, Mesure = `nb_articles`, Anneau intérieur = `source`).

**Titre** : `Répartition du flux par catégorie et par source (7 jours)`

```sql
SELECT
    c.category_slug   AS categorie,
    s.source_name     AS source,
    COUNT(*)          AS nb_articles
FROM gold.fact_article f
JOIN gold.dim_date     d ON d.date_sk     = f.date_sk
JOIN gold.dim_category c ON c.category_sk = f.category_sk
JOIN gold.dim_source   s ON s.source_sk   = f.source_sk
WHERE d.date_complete >= CURRENT_DATE - 6
GROUP BY c.category_slug, s.source_name
ORDER BY nb_articles DESC;
```

---

## 4. Créneau horaire — Densité de publication par heure de la journée

**Justification** : la partie « à quelles heures le volume est-il le plus
dense » — répond directement à la décision « à quelle heure envoyer la
newsletter » (juste après le pic de publication du matin, typiquement).

**Type** : Courbe (Abscisse = `heure`, Série = `tranche_horaire`, Ordonnées = `nb_articles`)

**Titre** : `Volume de publication par heure de la journée (7 jours, UTC)`

```sql
SELECT
    h.heure_sk          AS heure,
    h.tranche_horaire,
    COUNT(*)            AS nb_articles
FROM gold.fact_article f
JOIN gold.dim_date  d ON d.date_sk  = f.date_sk
JOIN gold.dim_heure h ON h.heure_sk = f.heure_sk
WHERE d.date_complete >= CURRENT_DATE - 6
GROUP BY h.heure_sk, h.tranche_horaire
ORDER BY h.heure_sk;
```

> Note pour la légende (exigence « lisible seul ») : préciser **UTC** dans le
> titre, car `dim_heure` est peuplée depuis `published_at AT TIME ZONE 'UTC'`.
> Pour un affichage en heure de Paris, remplacer `'UTC'` par
> `'Europe/Paris'` dans `load_gold` — mais le faire AVANT de charger les
> données, et de façon cohérente entre `date_sk` et `heure_sk`.

---

## Annexe (hors question métier) — Santé du pipeline

Facultatif : une 5ᵉ carte à destination de vous-mêmes, pas de la responsable
de veille. Utile pendant la démo pour montrer le chemin d'échec qualité.

**Type** : Table. **Titre** : `Derniers contrôles qualité par run`

```sql
SELECT run_id, rule_name, records_checked, records_failed, passed, logged_at
FROM logs.quality_log
ORDER BY logged_at DESC
LIMIT 25;
```

---

## Check final contre la partie 6 de l'énoncé

- [x] Chaque visualisation se justifie par rapport à la question métier
      (justification écrite au-dessus de chaque requête — recopiez-la dans la
      description de la question Metabase).
- [x] 2 à 3 visualisations pertinentes et non redondantes : une évolution
      temporelle (2), une comparaison catégories/dimensions (3), un indicateur
      agrégé clé (1) — exactement les trois exemples de l'énoncé, plus le
      créneau horaire (4) qui porte la décision finale.
- [x] Titres et légendes clairs : titres fournis, unités et fuseau précisés,
      le dashboard se lit sans commentaire oral.