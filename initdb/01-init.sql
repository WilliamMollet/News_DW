-- =============================================================================
-- Init du Data Warehouse (exécuté une seule fois à la création du volume)
-- =============================================================================

-- Base applicative de Metabase (séparée du DW)
CREATE DATABASE metabase OWNER dw;

-- Schémas des zones du lakehouse côté Postgres
\connect newsdw

CREATE SCHEMA IF NOT EXISTS silver;      -- données nettoyées
CREATE SCHEMA IF NOT EXISTS quarantine;  -- lignes rejetées par les règles qualité
CREATE SCHEMA IF NOT EXISTS gold;        -- schéma en étoile (fact + dims)
CREATE SCHEMA IF NOT EXISTS logs;        -- logs métier ET techniques

-- Squelette des tables de logs (à adapter si besoin)
CREATE TABLE IF NOT EXISTS logs.technical_log (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    level        TEXT NOT NULL,          -- INFO / WARNING / ERROR
    message      TEXT NOT NULL,
    logged_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS logs.quality_log (
    id             BIGSERIAL PRIMARY KEY,
    run_id         TEXT NOT NULL,
    rule_name      TEXT NOT NULL,        -- completude / exactitude / coherence / fraicheur / unicite
    records_checked  INTEGER NOT NULL,
    records_failed   INTEGER NOT NULL,
    passed         BOOLEAN NOT NULL,
    details        JSONB,
    logged_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);