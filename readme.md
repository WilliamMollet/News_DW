# TP Lakehouse + Airflow + Metabase — environnement

## Démarrage

```bash
echo "AIRFLOW_UID=$(id -u)" > .env   # Linux uniquement (évite les problèmes de droits sur ./logs)
docker compose up -d
```

Premier démarrage : ~2-3 min (migration Airflow, installation des paquets pip, init Metabase).

## Accès

| Service            | URL                    | Identifiants        |
|--------------------|------------------------|---------------------|
| Airflow UI         | http://localhost:8080  | airflow / airflow   |
| MinIO Console      | http://localhost:9001  | minio / minio12345  |
| Metabase           | http://localhost:3000  | setup au 1er accès  |
| Postgres DW (hôte) | localhost:5433         | dw / dw, base `newsdw` |

## Architecture

- `postgres-airflow` : metadata DB **dédiée** d'Airflow (LocalExecutor, pas de SQLite).
- `minio` : zone **bronze** (JSON brut de l'API Noozra), buckets `bronze/silver/gold` créés automatiquement.
- `postgres-dw` : base `newsdw` avec schémas `silver`, `quarantine`, `gold` (étoile), `logs`.
- `metabase` : branché sur `postgres-dw` (sa base applicative `metabase` y est créée à l'init).

## Connexion Metabase → DW (au setup)

- Type : PostgreSQL — Host : `postgres-dw` — Port : `5432`
- Base : `newsdw` — Utilisateur : `dw` — Mot de passe : `dw`

## Vos DAGs

À déposer dans `./dags/`. Les variables d'environnement suivantes sont déjà
injectées dans les conteneurs Airflow : `NOOZRA_BASE_URL`, `MINIO_ENDPOINT`,
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `DW_CONN`.

## Réinitialisation complète

```bash
docker compose down -v   # supprime aussi les volumes (données + metadata)
```