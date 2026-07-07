"""
DAG news_pipeline — TP autonome : Pipeline Lakehouse + Airflow + Dashboard métier
=================================================================================

Source      : API Noozra (https://noozra.com/api) — publique, sans clé, 100 req/j/IP.
Architecture: extract (4 catégories en PARALLÈLE, LocalExecutor)
              -> load_bronze (JSON brut dans MinIO)
              -> quality_checks (5 règles, loggées séparément dans logs.quality_log)
              -> transform_silver (lignes valides)  +  quarantine (lignes rejetées)
              -> load_gold (schéma en étoile, idempotent : ON CONFLICT DO NOTHING)
              -> quality_report (bilan du run)

Démonstration des 3 chemins (livrable 3)
----------------------------------------
1. NOMINAL          : déclencher le DAG tel quel -> DAG vert, données en gold.
2. ÉCHEC QUALITÉ    : dans l'UI Airflow, Admin > Variables, créer
                      news_inject_bad_data = true
                      puis déclencher le DAG -> le DAG reste VERT, les lignes
                      corrompues partent dans quarantine.articles_rejetes.
                      (Remettre la variable à false ensuite.)
3. ÉCHEC TECHNIQUE  : créer la variable
                      noozra_base_url = https://noozra.com/api-inexistant
                      puis déclencher le DAG -> les tâches extract échouent
                      après 2 retries -> DAG ROUGE, erreur tracée dans
                      logs.technical_log. (Supprimer la variable ensuite.)

Logging à deux niveaux : logs Airflow natifs (UI, par tâche) ET logs métier
persistés en base (logs.technical_log, logs.quality_log).
"""

from __future__ import annotations

import io
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
CATEGORIES = ["world", "sports", "tech", "business"]  # 4 extractions parallèles
PAGE_LIMIT = 100                 # max autorisé par l'API
API_TIMEOUT_S = 15               # timeout réseau interne à la tâche
FRESHNESS_MAX_HOURS = 24         # règle de fraîcheur (règle de batch)
CLOCK_SKEW = timedelta(minutes=5)
URL_RE = re.compile(r"^https?://")
REQUIRED_FIELDS = ("id", "headline", "url", "published_at", "source", "category")

BRONZE_BUCKET = "bronze"

# Robustesse exigée par l'énoncé — appliquée à TOUTES les tâches du DAG
DEFAULT_ARGS = {
    "retries": 2,                                  # plafonné : quota API 100 req/j
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(seconds=60),
}


# ----------------------------------------------------------------------------
# Helpers (connexions + logging métier)
# ----------------------------------------------------------------------------
def _base_url() -> str:
    """URL de l'API. Surchargeable via la Variable Airflow `noozra_base_url`
    (c'est le levier de démonstration de l'échec technique)."""
    return Variable.get(
        "noozra_base_url",
        default_var=os.environ.get("NOOZRA_BASE_URL", "https://noozra.com/api"),
    ).rstrip("/")


def _dw_conn():
    import psycopg2
    return psycopg2.connect(os.environ["DW_CONN"])


def _minio_client():
    from minio import Minio
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def _ctx():
    ctx = get_current_context()
    return ctx["run_id"], ctx["ti"].task_id


def _tech_log(level: str, message: str) -> None:
    """Log métier technique, persisté en base (en plus du log Airflow natif)."""
    run_id, task_id = _ctx()
    with _dw_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO logs.technical_log (run_id, task_id, level, message) "
            "VALUES (%s, %s, %s, %s)",
            (run_id, task_id, level, message),
        )


def _quality_log(rule: str, checked: int, failed: int, passed: bool, details: dict) -> None:
    """Une ligne par règle qualité et par run — exigence : loggées séparément."""
    run_id, _ = _ctx()
    with _dw_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO logs.quality_log "
            "(run_id, rule_name, records_checked, records_failed, passed, details) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (run_id, rule, checked, failed, passed, json.dumps(details)),
        )


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# ----------------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------------
@dag(
    dag_id="news_pipeline",
    description="Lakehouse Noozra : bronze MinIO -> silver/quarantaine -> gold étoile -> Metabase",
    schedule="@hourly",
    start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["tp", "lakehouse", "noozra"],
)
def news_pipeline():

    # ------------------------------------------------------------------ DDL --
    @task
    def init_tables() -> None:
        """DDL idempotent (IF NOT EXISTS) : silver, quarantaine, gold, logs."""
        ddl = """
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS quarantine;
        CREATE SCHEMA IF NOT EXISTS gold;
        CREATE SCHEMA IF NOT EXISTS logs;

        CREATE TABLE IF NOT EXISTS logs.technical_log (
            id BIGSERIAL PRIMARY KEY, run_id TEXT NOT NULL, task_id TEXT NOT NULL,
            level TEXT NOT NULL, message TEXT NOT NULL,
            logged_at TIMESTAMPTZ NOT NULL DEFAULT now());

        CREATE TABLE IF NOT EXISTS logs.quality_log (
            id BIGSERIAL PRIMARY KEY, run_id TEXT NOT NULL, rule_name TEXT NOT NULL,
            records_checked INTEGER NOT NULL, records_failed INTEGER NOT NULL,
            passed BOOLEAN NOT NULL, details JSONB,
            logged_at TIMESTAMPTZ NOT NULL DEFAULT now());

        CREATE TABLE IF NOT EXISTS silver.articles (
            article_id UUID PRIMARY KEY,
            headline TEXT NOT NULL,
            url TEXT NOT NULL,
            source_name TEXT NOT NULL,
            category_slug TEXT NOT NULL,
            published_at TIMESTAMPTZ NOT NULL,
            has_image BOOLEAN NOT NULL,
            description_length INTEGER NOT NULL,
            image_width INTEGER,
            image_height INTEGER,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT now());

        CREATE TABLE IF NOT EXISTS quarantine.articles_rejetes (
            id BIGSERIAL PRIMARY KEY,
            raw JSONB NOT NULL,
            reasons TEXT[] NOT NULL,
            run_id TEXT NOT NULL,
            rejected_at TIMESTAMPTZ NOT NULL DEFAULT now());

        CREATE TABLE IF NOT EXISTS gold.dim_source (
            source_sk SERIAL PRIMARY KEY,
            source_name TEXT NOT NULL UNIQUE);

        CREATE TABLE IF NOT EXISTS gold.dim_category (
            category_sk SERIAL PRIMARY KEY,
            category_slug TEXT NOT NULL UNIQUE);

        CREATE TABLE IF NOT EXISTS gold.dim_heure (
            heure_sk INTEGER PRIMARY KEY CHECK (heure_sk BETWEEN 0 AND 23),
            tranche_horaire TEXT NOT NULL);

        CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_sk INTEGER PRIMARY KEY,
            date_complete DATE NOT NULL,
            annee INTEGER NOT NULL, mois INTEGER NOT NULL, jour INTEGER NOT NULL,
            nom_jour_semaine TEXT NOT NULL,
            numero_semaine_iso INTEGER NOT NULL,
            est_weekend BOOLEAN NOT NULL);

        CREATE TABLE IF NOT EXISTS gold.fact_article (
            article_id UUID PRIMARY KEY,
            date_sk INTEGER NOT NULL REFERENCES gold.dim_date(date_sk),
            heure_sk INTEGER NOT NULL REFERENCES gold.dim_heure(heure_sk),
            source_sk INTEGER NOT NULL REFERENCES gold.dim_source(source_sk),
            category_sk INTEGER NOT NULL REFERENCES gold.dim_category(category_sk),
            headline TEXT NOT NULL,
            url TEXT NOT NULL,
            has_image BOOLEAN NOT NULL,
            description_length INTEGER NOT NULL,
            image_width INTEGER, image_height INTEGER,
            published_at TIMESTAMPTZ NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            run_id TEXT NOT NULL);
        """
        with _dw_conn() as conn, conn.cursor() as cur:
            cur.execute(ddl)
        _tech_log("INFO", "DDL silver/quarantine/gold/logs vérifié (IF NOT EXISTS).")

    # -------------------------------------------------------------- EXTRACT --
    @task
    def extract(category: str) -> dict:
        """Appel API pour UNE catégorie. Les 4 instances tournent en parallèle
        (LocalExecutor). Chemin d'échec technique : URL invalide ou timeout
        -> exception -> 2 retries -> tâche rouge -> DAG rouge."""
        url = f"{_base_url()}/articles"
        try:
            resp = requests.get(
                url,
                params={"category": category, "limit": PAGE_LIMIT},
                timeout=API_TIMEOUT_S,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            _tech_log("ERROR", f"Échec extraction '{category}' sur {url} : {exc}")
            raise
        count = len(payload.get("articles", []))
        _tech_log("INFO", f"Extraction '{category}' : {count} articles reçus.")
        return {"category": category, "payload": payload}

    # ---------------------------------------------------------------- BRONZE --
    @task
    def load_bronze(extracts: list[dict]) -> list[str]:
        """Écrit le JSON BRUT (aucune transformation) dans MinIO.
        Clé : noozra/dt=<date>/run=<run_id>/<categorie>.json
        Si la Variable `news_inject_bad_data` vaut true, on injecte des
        enregistrements corrompus DANS le bronze (démo du chemin qualité)."""
        run_id, _ = _ctx()
        ds = get_current_context()["ds"]
        inject = Variable.get("news_inject_bad_data", default_var="false").lower() == "true"

        if inject:
            bad = [
                {  # complétude : headline manquant
                    "id": str(uuid.uuid4()), "headline": None,
                    "url": "https://example.com/sans-titre",
                    "published_at": datetime.now(timezone.utc).isoformat(),
                    "source": "Test Injecteur", "category": "general",
                    "description": "", "image_url": None,
                    "image_width": None, "image_height": None,
                },
                {  # cohérence : catégorie inconnue + date future
                    "id": str(uuid.uuid4()), "headline": "Article venu du futur",
                    "url": "https://example.com/futur",
                    "published_at": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
                    "source": "Test Injecteur", "category": "licornes",
                    "description": "donnée corrompue volontairement",
                    "image_url": None, "image_width": None, "image_height": None,
                },
            ]
            extracts[0]["payload"].setdefault("articles", []).extend(bad)
            _tech_log("WARNING", "Injection de 2 enregistrements corrompus (démo qualité).")

        client = _minio_client()
        keys = []
        for item in extracts:
            key = f"noozra/dt={ds}/run={run_id}/{item['category']}.json"
            data = json.dumps(item["payload"], ensure_ascii=False).encode("utf-8")
            client.put_object(BRONZE_BUCKET, key, io.BytesIO(data), len(data),
                              content_type="application/json")
            keys.append(key)
        _tech_log("INFO", f"Bronze : {len(keys)} objets écrits dans MinIO ({keys}).")
        return keys

    # --------------------------------------------------------------- QUALITÉ --
    @task
    def quality_checks(bronze_keys: list[str]) -> dict:
        """Applique les 5 règles qualité sur le batch bronze.
        Chaque règle est loggée SÉPARÉMENT dans logs.quality_log.
        Les lignes invalides partent en quarantaine ; le DAG reste VERT."""
        client = _minio_client()
        articles: list[dict] = []
        for key in bronze_keys:
            obj = client.get_object(BRONZE_BUCKET, key)
            try:
                articles.extend(json.loads(obj.read()).get("articles", []))
            finally:
                obj.close()
                obj.release_conn()

        # Référentiel officiel des catégories (règle de cohérence)
        resp = requests.get(f"{_base_url()}/categories", timeout=API_TIMEOUT_S)
        resp.raise_for_status()
        raw_cats = resp.json()
        known_cats = {
            (c.get("slug") or c.get("name") or str(c)).lower() if isinstance(c, dict) else str(c).lower()
            for c in (raw_cats if isinstance(raw_cats, list) else raw_cats.get("categories", []))
        }

        now = datetime.now(timezone.utc)
        total = len(articles)
        fails = {"completude": 0, "exactitude": 0, "coherence": 0, "unicite": 0}
        samples: dict[str, list] = {k: [] for k in fails}
        valid, rejected = [], []
        seen_ids: set[str] = set()

        # ids déjà présents en silver (unicité inter-runs -> simple skip, pas quarantaine)
        with _dw_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT article_id::text FROM silver.articles")
            in_silver = {r[0] for r in cur.fetchall()}

        skipped_existing = 0
        for art in articles:
            reasons = []

            # Règle 1 — complétude
            missing = [f for f in REQUIRED_FIELDS if not art.get(f)]
            if missing:
                reasons.append(f"completude:champs manquants {missing}")
                fails["completude"] += 1
                samples["completude"].append(art.get("id"))

            # Règle 2 — exactitude
            exact_errs = []
            if art.get("url") and not URL_RE.match(str(art["url"])):
                exact_errs.append("url invalide")
            pub_dt = None
            if art.get("published_at"):
                try:
                    pub_dt = _parse_dt(art["published_at"])
                except (ValueError, TypeError):
                    exact_errs.append("published_at non parseable")
            for dim in ("image_width", "image_height"):
                v = art.get(dim)
                if v is not None and (not isinstance(v, (int, float)) or v <= 0):
                    exact_errs.append(f"{dim} <= 0")
            if exact_errs:
                reasons.append("exactitude:" + ",".join(exact_errs))
                fails["exactitude"] += 1
                samples["exactitude"].append(art.get("id"))

            # Règle 3 — cohérence
            coher_errs = []
            if art.get("category") and known_cats and str(art["category"]).lower() not in known_cats:
                coher_errs.append(f"categorie inconnue '{art['category']}'")
            if pub_dt and pub_dt > now + CLOCK_SKEW:
                coher_errs.append("published_at dans le futur")
            if coher_errs:
                reasons.append("coherence:" + ",".join(coher_errs))
                fails["coherence"] += 1
                samples["coherence"].append(art.get("id"))

            # Règle 5 — unicité (intra-batch : rejet ; inter-run : skip idempotent)
            aid = art.get("id")
            if aid and aid in seen_ids:
                reasons.append("unicite:doublon intra-batch")
                fails["unicite"] += 1
                samples["unicite"].append(aid)
            elif aid and aid in in_silver:
                skipped_existing += 1
                continue  # déjà connu : ni valide ni rejeté, simplement ignoré
            if aid:
                seen_ids.add(aid)

            (rejected if reasons else valid).append(
                {"article": art, "reasons": reasons} if reasons else art
            )

        # Logs séparés, une ligne par règle
        _quality_log("completude", total, fails["completude"],
                     fails["completude"] == 0, {"exemples_ids": samples["completude"][:5]})
        _quality_log("exactitude", total, fails["exactitude"],
                     fails["exactitude"] == 0, {"exemples_ids": samples["exactitude"][:5]})
        _quality_log("coherence", total, fails["coherence"],
                     fails["coherence"] == 0, {"exemples_ids": samples["coherence"][:5],
                                               "referentiel": sorted(known_cats)})
        _quality_log("unicite", total, fails["unicite"],
                     fails["unicite"] == 0, {"doublons_intra_batch": samples["unicite"][:5],
                                             "deja_en_silver_ignores": skipped_existing})

        # Règle 4 — fraîcheur (règle de BATCH : warning, pas de quarantaine)
        parseable_dates = []
        for a in articles:
            try:
                parseable_dates.append(_parse_dt(a["published_at"]))
            except (KeyError, ValueError, TypeError, AttributeError):
                continue  # déjà signalé par la règle d'exactitude
        newest = max(parseable_dates, default=None)
        fresh_ok = newest is not None and (now - newest) <= timedelta(hours=FRESHNESS_MAX_HOURS)
        _quality_log("fraicheur", total, 0 if fresh_ok else 1, fresh_ok,
                     {"article_le_plus_recent": newest.isoformat() if newest else None,
                      "seuil_heures": FRESHNESS_MAX_HOURS})
        if not fresh_ok:
            _tech_log("WARNING", "Règle fraîcheur non satisfaite : flux en retard ?")

        _tech_log("INFO", f"Qualité : {total} lus, {len(valid)} valides, "
                          f"{len(rejected)} rejetés, {skipped_existing} déjà en silver.")
        return {"valid": valid, "rejected": rejected,
                "skipped_existing": skipped_existing, "total": total}

    # ----------------------------------------------------------- QUARANTAINE --
    @task
    def quarantine_rejects(qc: dict) -> int:
        """Insère les lignes rejetées + motifs. Le DAG reste vert : la donnée
        mauvaise est isolée, pas le pipeline."""
        run_id, _ = _ctx()
        rejected = qc["rejected"]
        if rejected:
            with _dw_conn() as conn, conn.cursor() as cur:
                for item in rejected:
                    cur.execute(
                        "INSERT INTO quarantine.articles_rejetes (raw, reasons, run_id) "
                        "VALUES (%s, %s, %s)",
                        (json.dumps(item["article"], ensure_ascii=False),
                         item["reasons"], run_id),
                    )
        _tech_log("INFO", f"Quarantaine : {len(rejected)} lignes isolées.")
        return len(rejected)

    # ---------------------------------------------------------------- SILVER --
    @task
    def transform_silver(qc: dict) -> int:
        """Typage + normalisation des lignes valides, insertion en silver."""
        run_id, _ = _ctx()
        rows = 0
        with _dw_conn() as conn, conn.cursor() as cur:
            for art in qc["valid"]:
                cur.execute(
                    """
                    INSERT INTO silver.articles
                        (article_id, headline, url, source_name, category_slug,
                         published_at, has_image, description_length,
                         image_width, image_height, run_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_id) DO NOTHING
                    """,
                    (
                        art["id"],
                        art["headline"].strip(),
                        art["url"].strip(),
                        art["source"].strip(),
                        art["category"].strip().lower(),
                        _parse_dt(art["published_at"]),
                        bool(art.get("image_url")),
                        len(art.get("description") or ""),
                        art.get("image_width"),
                        art.get("image_height"),
                        run_id,
                    ),
                )
                rows += cur.rowcount
        _tech_log("INFO", f"Silver : {rows} nouvelles lignes insérées.")
        return rows

    # ------------------------------------------------------------------ GOLD --
    @task
    def load_gold(silver_new_rows: int) -> int:
        """Chargement du schéma en étoile depuis silver, entièrement idempotent
        (dims et fact en ON CONFLICT DO NOTHING). Rejouer le DAG ne crée
        aucun doublon — vérifiable avec les requêtes du livrable 4."""
        run_id, _ = _ctx()
        with _dw_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gold.dim_heure (heure_sk, tranche_horaire)
                SELECT h, CASE WHEN h < 6  THEN 'nuit'
                               WHEN h < 12 THEN 'matin'
                               WHEN h < 18 THEN 'apres-midi'
                               ELSE 'soiree' END
                FROM generate_series(0, 23) AS h
                ON CONFLICT (heure_sk) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO gold.dim_date (date_sk, date_complete, annee, mois, jour,
                                           nom_jour_semaine, numero_semaine_iso, est_weekend)
                SELECT DISTINCT
                    to_char(published_at AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
                    (published_at AT TIME ZONE 'UTC')::date,
                    EXTRACT(YEAR  FROM published_at AT TIME ZONE 'UTC')::int,
                    EXTRACT(MONTH FROM published_at AT TIME ZONE 'UTC')::int,
                    EXTRACT(DAY   FROM published_at AT TIME ZONE 'UTC')::int,
                    trim(to_char(published_at AT TIME ZONE 'UTC', 'Day')),
                    EXTRACT(WEEK  FROM published_at AT TIME ZONE 'UTC')::int,
                    EXTRACT(ISODOW FROM published_at AT TIME ZONE 'UTC') IN (6, 7)
                FROM silver.articles
                ON CONFLICT (date_sk) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO gold.dim_source (source_name)
                SELECT DISTINCT source_name FROM silver.articles
                ON CONFLICT (source_name) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO gold.dim_category (category_slug)
                SELECT DISTINCT category_slug FROM silver.articles
                ON CONFLICT (category_slug) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO gold.fact_article
                    (article_id, date_sk, heure_sk, source_sk, category_sk,
                     headline, url, has_image, description_length,
                     image_width, image_height, published_at, run_id)
                SELECT
                    a.article_id,
                    to_char(a.published_at AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
                    EXTRACT(HOUR FROM a.published_at AT TIME ZONE 'UTC')::int,
                    s.source_sk,
                    c.category_sk,
                    a.headline, a.url, a.has_image, a.description_length,
                    a.image_width, a.image_height, a.published_at, %s
                FROM silver.articles a
                JOIN gold.dim_source   s ON s.source_name   = a.source_name
                JOIN gold.dim_category c ON c.category_slug = a.category_slug
                ON CONFLICT (article_id) DO NOTHING;
            """, (run_id,))
            inserted = cur.rowcount
        _tech_log("INFO", f"Gold : {inserted} nouveaux faits (upsert idempotent).")
        return inserted

    # ---------------------------------------------------------------- BILAN --
    @task
    def quality_report(qc: dict, silver_rows: int, gold_rows: int, quarantined: int) -> None:
        """Bilan métier du run, persisté en logs.technical_log."""
        msg = (f"Bilan run : {qc['total']} articles lus | {silver_rows} en silver | "
               f"{gold_rows} nouveaux faits gold | {quarantined} en quarantaine | "
               f"{qc['skipped_existing']} déjà connus (idempotence).")
        print(msg)  # log Airflow natif
        _tech_log("INFO", msg)  # log métier persisté

    # ------------------------------------------------------------- WIRING ----
    init = init_tables()
    extracts = [extract.override(task_id=f"extract_{c}")(c) for c in CATEGORIES]
    init >> extracts
    bronze_keys = load_bronze(extracts)
    qc = quality_checks(bronze_keys)
    quarantined = quarantine_rejects(qc)
    silver_rows = transform_silver(qc)
    gold_rows = load_gold(silver_rows)
    quality_report(qc, silver_rows, gold_rows, quarantined)


news_pipeline()