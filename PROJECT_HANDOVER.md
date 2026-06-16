# PROJECT HANDOVER — FerroFlux (Steel Supply-Chain V3)

> **Purpose of this document.** This is a complete, self-contained specification
> of the project. If you hand this project back later, this file alone should let
> anyone (or any AI assistant) understand the whole system and rebuild *every*
> deliverable from scratch — the Spark pipeline, the FastAPI portal, the n8n
> automation, and the Docker topology — without needing the prior conversation.
## 0. One-paragraph summary

FerroFlux is a multi-tenant Big-Data analytics platform for Egyptian steel
supply-chain optimization. Real-time market/factory streams (Kafka) and batch
CSV uploads land in a Spark **medallion** pipeline (Bronze → Silver → Gold) that
writes Parquet + PostgreSQL marts. An **Airflow** DAG orchestrates the batch ETL.
A **FastAPI** portal (`steel-portal`) serves two modes: a public portfolio + live
demo (Mode A), and a secure, row-isolated tenant SaaS workspace (Mode B). An
**n8n** layer handles anomaly alerting (inbound webhook from Spark) and a daily
market-price harvest (cron → Kafka). Everything runs as Docker services on a
single bridge network, `steel-network`.

---

## 1. Architecture & container topology

All services share one Docker bridge network: **`steel-network`**. They resolve
each other by service name (DNS). Defined across `docker-compose.yml` +
`docker-compose.override.yml`.

| Service | Image / build | Role | Ports |
|---|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper | Kafka coordination | 2181 |
| `kafka` | confluentinc/cp-kafka:7.4.0 | Streams. **Internal listener `kafka:29092`**, external `localhost:9092` | 9092 |
| `kafka-ui` | provectuslabs/kafka-ui | Topic inspection (`KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092`) | 8080→ |
| `spark-master` | build `Dockerfile.spark` | Medallion batch + structured streaming | 7077/8081 |
| `spark-worker` | build `Dockerfile.spark` | Executor capacity | — |
| `steel-postgres` | postgres | Warehouse `steel_db` (schemas: raw_data, processed_data, analytics, tenants) | 5432 |
| `airflow-postgres` | postgres | Airflow metadata DB `airflow` | — |
| `airflow-redis` | redis | Airflow broker | — |
| `airflow-init/webserver/scheduler` | build `Dockerfile` (Airflow base) | LocalExecutor DAG orchestration | 8080 |
| `pgadmin` | dpage/pgadmin4 | DB admin UI | — |
| `steel-portal` | build `portal/Dockerfile` | **The FastAPI app (this project's portal)** | 8000 |
| `n8n` | n8nio/n8n:latest | Automation: alerting webhook + market cron | 5678 |

**Network-wide canonical strings** (must stay consistent everywhere):
- JDBC: `jdbc:postgresql://steel-postgres:5432/steel_db`
- DB creds: user `steel_admin`, password `steel_pass_2024`, db `steel_db`
- Kafka broker (in-network): `kafka:29092`
- n8n webhook: `http://n8n:5678/webhook/steel-alert`
- Airflow API: `http://airflow-webserver:8080/api/v1`, user `admin` / `admin123`
- ETL DAG id: `steel_production_etl`

---

## 2. Data model (PostgreSQL `steel_db`)

Two SQL files build the schema, applied in order:

1. **`init_db.sql`** — base schema. Tables in `raw_data`, `processed_data`,
   `analytics`. Originally single-column PKs, **no tenant columns**.
2. **`01_multitenant_migration.sql`** — adds the `tenants` schema and multi-tenancy:
   - `tenants.companies(company_id PK, company_name, …)`
   - `tenants.factories(factory_id PK, company_id FK, factory_name, governorate)`
   - `tenants.users(user_id, username UNIQUE, password_hash, company_id, factory_id, role)`
   - Adds `company_id` + `factory_id` columns to analytics/processed tables and
     promotes their PKs to **composite** `(company_id, factory_id, <natural key>)`.

**Analytics marts the portal reads** (all tenant-scoped):
- `analytics.daily_kpis` — date, total_production_tons, total_revenue_egp,
  total_orders, avg_efficiency, total_co2_kg, profit_estimate_egp, steel_price_egp
- `analytics.monthly_summary`
- `analytics.supplier_scorecard` — supplier_name, origin_country, material_type,
  total_purchases, avg_price_per_ton_usd, avg_lead_time_days, on_time_pct, risk_score
- `analytics.regional_demand` — governorate, region, total_orders,
  total_quantity_tons, total_revenue_egp, avg_delivery_days, delay_pct, top_product
- `analytics.production_efficiency` — facility, production_line, line_type,
  total_output_tons, avg_efficiency, downtime_pct, best_shift, worst_shift
- `processed_data.market_clean` — date, steel_price_egypt_egp, price_change_pct,
  is_price_spike (used for the price-alert badge)

Seeded demo tenant: company `EZZ` ("EZZ Steel Group"), factory `EZZ_DEMO`.

---

## 3. The Spark medallion pipeline (`scripts/spark_jobs/`)

Layout on disk: `data/processed/{bronze,silver,gold}/<table>/…parquet`.

### 3.1 Layers
- **`bronze_etl.py`** (unchanged original) — ingests `data/raw/*.csv` + Kafka into
  immutable Parquet (`data/processed/bronze/*`) and a `raw_data.*` mirror.
- **`silver_etl.py`** (REFACTORED) — cleans/enriches: FX conversions, governorate→region
  mapping, efficiency & delay scoring, `is_price_spike` flagging. Writes
  `data/processed/silver/*` and `processed_data.*`. Emits `price_spike` and
  `low_efficiency` anomalies to n8n.
- **`gold_etl.py`** (REFACTORED) — aggregates 6 marts into `data/processed/gold/*`
  and `analytics.*`. Emits `line_efficiency_low` anomalies.
- **`streaming_etl.py`** (original) — Spark Structured Streaming consuming Kafka
  topic `steel_market_prices` for the live view.
- **`etl_common.py`** (NEW — the heart of the refactor, see §3.3).

### 3.2 Unified partition strategy (tenant-first)
| Table | Partition columns |
|---|---|
| market_clean | `year` |
| production_clean | `company_id, factory_id, facility` |
| orders_clean | `company_id, factory_id, region` |
| shipments_clean | `company_id, factory_id, transport_mode` |
| rawmat_clean | `company_id, factory_id` |
| daily_kpis | `company_id, factory_id, year` |
| monthly_summary | `company_id, factory_id, year_val, month_val` |
| supplier_scorecard | `company_id, factory_id, material_type` |
| regional_demand | `company_id, factory_id, region` |
| production_efficiency | `company_id, factory_id, facility` |
| price_features | `year` |

### 3.3 THE PARTITIONING BUG (root cause + fix) — most important section
**Symptom:** `java.lang.AssertionError: Conflicting partition column names detected`
when Gold ran `spark.read.parquet(<table>)`.

**Root cause:** every Silver/Gold write used
`.mode("overwrite").option("partitionOverwriteMode","dynamic")`. **Dynamic**
overwrite only replaces the partition directories present in the *new* DataFrame;
it never deletes stale directories from a previous run that used a *different*
partition scheme. So a legacy layout like `orders_clean/region=Delta/…` could
coexist with the new `orders_clean/company_id=EZZ/factory_id=…/region=Delta/…`.
Two different partition **depths** under one path → Spark's reader asserts.

**Fix (in `etl_common.py`):** `purge_partitions()` is called **before every write**:
- A **global/full** run wipes the table directory entirely (no stale scheme can survive).
- A **tenant-scoped** run removes only that tenant's subtree, plus any legacy
  top-level directory whose first partition key ≠ the expected `company_id`.
Then `write_partitioned()` writes the standardized layout, so depth is always uniform.

### 3.4 Other Spark fixes
- **`supplier_scorecard` AnalysisException:** the original `.select(...)` dropped
  `company_id`/`factory_id` then `.partitionBy(...)` on them → fixed by keeping the
  tenant columns in the projection.
- **Divide-by-zero** in stat prints / `gross_margin` → guarded (`safe_pct`, zero-revenue guards).
- **Hard-coded JDBC** → env-driven via `pg_conf()/jdbc_url()/jdbc_props()` reading
  `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD` (defaults `steel-postgres/5432/steel_db/steel_admin/steel_pass_2024`).

### 3.5 `etl_common.py` API (rebuild reference)
`pg_conf()`, `jdbc_url()`, `jdbc_props()`; `tenant_scope()`, `apply_scope(df)`;
`purge_partitions(path, expected_first_col, scope)` (THE FIX);
`write_partitioned(df, path, cols, scope)`; `save_pg_tenant(df, table, scope)`
(delete-by-tenant then append, parameterized); `notify_n8n(event_type, payload)`
and `emit_anomalies_from_df(df, …)` (POST to `N8N_WEBHOOK_URL`, swallow failures);
`safe_pct(num, den)`.

---

## 4. The portal (`portal/app/`, FastAPI) — branded "FerroFlux"

### 4.1 Files
- `main.py` — app + routes (both modes), Jinja2 templates at `app/templates`.
- `config.py` — `Settings`: `_build_database_url()` (from `DATABASE_URL` or parts,
  password defaults to `steel_pass_2024`), `SECRET_KEY`, `TOKEN_TTL`, `ADMIN_USER/PASS`,
  `AIRFLOW_*`, `ETL_DAG_ID`, `N8N_WEBHOOK_URL`, `KAFKA_BOOTSTRAP`.
- `database.py` — psycopg2 `get_db_connection()` (retry+backoff), `fetch_all/fetch_one`
  (RealDictCursor, parameterized, return `[]` on error), `healthcheck()`.
- `security.py` — **the isolation boundary.** HMAC-SHA256 signed tokens
  (`issue_token/verify_token`, exp check), `hash_password/verify_password`
  (bcrypt + sha256 fallback), `TenantContext.where(alias)` → parameterized
  `company_id=%s [AND factory_id=%s]` fragment+params, `current_tenant()` dependency
  (Bearer header), `require_admin()`.
- `routers/auth.py` — `/login`, `/register` (idempotent `ON CONFLICT` upserts deriving
  `company_id` from factory name), `/demo`.
- `routers/dashboard.py` — `/summary /daily /production-lines /regional-demand
  /suppliers /price-alerts`; every query uses `tenant.where()` bound params.
- `routers/factories.py` — `GET/POST` factory units; `company_id` from token (never body);
  `VALID_GOVERNORATES` whitelist; `ON CONFLICT (factory_id) DO UPDATE`.
- `routers/upload.py` — `POST /excel/{data_type}`, `GET /templates/{data_type}`;
  `ALLOWED_TYPES` = market/production/orders/shipments/raw_materials (each with
  `required_cols` + target `raw_data.*` table); tenant columns stripped then re-tagged;
  env-driven Airflow DAG trigger.
- `templates/{portfolio,demo,login,dashboard}.html` — see §4.4.

### 4.2 Mode A — public
- `GET /` → `portfolio.html` (architecture / pipeline / topology story)
- `GET /demo` → `demo.html` (Chart.js live demo)
- `GET /api/public/metrics` → synthetic 60-day series:
  `{date, steel_price_egp, throughput_tons, supply_chain_latency_days}` + headline
  `{latest_price, latest_throughput, price_spike}`.

### 4.3 Mode B — secure
- `GET /login` → `login.html`; `GET /app` → `dashboard.html` (SPA shell)
- API under `/api/auth`, `/api/dashboard`, `/api/factories`, `/api/upload`.
- **Isolation invariant:** the company/factory scope is read ONLY from the verified
  token; no endpoint trusts client-supplied scope. This is the security guarantee.
- Demo login: `admin` / `admin123`, or "enter as guest" (`/api/auth/demo`).

### 4.4 Front-end aesthetic (so a rebuild keeps the look)
Industrial "molten steel" theme. Fonts: **Anton** (display), **Hanken Grotesk**
(body), **JetBrains Mono** (data). Palette: graphite `#0c0d0f`, ember `#ff5a1f`,
molten `#ffb347`, cooled cyan `#4fd1c5`. CDN-only (Google Fonts, Chart.js 4.4.1).
The SPA stores the token in `localStorage` (`ff_token`, `ff_user`) and sends it as
`Authorization: Bearer …`.

### 4.5 Dockerfile (why it was broken)
The base compose built `steel-portal` off the Airflow `Dockerfile` with **no run
command** → no working entrypoint. Fixed with a dedicated `portal/Dockerfile`
(python:3.11-slim, installs `requirements.txt`, `CMD uvicorn app.main:app --host
0.0.0.0 --port 8000`, plus a `/health` HEALTHCHECK).

---

## 5. n8n automation (`n8n/`)

- **`workflow_1_realtime_alerting.json`** — Webhook (`steel-alert`) → Code (normalize)
  → Switch by `event_type` (`price_spike`→Slack, `low_efficiency`→Telegram,
  else→Email) → Respond. Receives `{event_type, payload:{company_id, factory_id,…}}`
  from the Spark jobs.
- **`workflow_2_market_harvest_cron.json`** — Schedule (daily 06:00) → HTTP fetch
  steel indices → Code (scrub/validate, reject empty) → Kafka publish to
  `steel_market_prices` on `kafka:29092`.
- **`README_n8n.md`** — import steps + credential setup (Slack OAuth, Telegram Bot,
  SMTP, Kafka broker `kafka:29092`). Note: production webhook URL only responds when
  the workflow is **activated**; otherwise use `/webhook-test/steel-alert`.

n8n itself was named in the spec but **missing** from the base compose — it is added
in `docker-compose.override.yml` (port 5678, persistent `n8n_data` volume).

---

## 6. The compose override (`docker-compose.override.yml`)
Compose auto-merges it on `docker compose up`. It (1) re-points `steel-portal` to
`portal/Dockerfile`, (2) fixes the `DATABASE_URL` password
(`steel_pass` → `steel_pass_2024`), (3) injects `N8N_WEBHOOK_URL` + `PG_*` into
`steel-portal`, `spark-master`, `spark-worker`, and (4) adds the `n8n` service.

---

## 7. Environment variables (the contract)
Portal: `DATABASE_URL` (or `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD`),
`PORTAL_SECRET_KEY`, `ADMIN_USER`, `ADMIN_PASS`, `AIRFLOW_BASE_URL/USER/PASSWORD`,
`ETL_DAG_ID`, `N8N_WEBHOOK_URL`, `KAFKA_BOOTSTRAP_SERVERS`.
Spark: `PG_*`, `N8N_WEBHOOK_URL`, plus per-run `FF_COMPANY`/`FF_FACTORY` (passed by the DAG).
Base `.env`: `POSTGRES_USER=steel_admin`, `POSTGRES_PASSWORD=steel_pass_2024`,
`AIRFLOW_DB_USER/PASS`.

---

## 8. Project file map (what lives where)
```
steel-supply-chainV3/
├── docker-compose.yml                 # base stack
├── docker-compose.override.yml        # REFACTOR: portal fix + n8n + env
├── Dockerfile / Dockerfile.spark      # Airflow base / Spark base
├── .env  .env.example                 # creds (steel_pass_2024)
├── init_db.sql                        # base schema
├── 01_multitenant_migration.sql       # tenants schema + composite PKs
├── REFACTOR_NOTES.md                  # change log + how-to-apply
├── PROJECT_HANDOVER.md                # THIS FILE
├── dags/steel_production_dag.py       # Airflow DAG id "steel_production_etl"
├── data/
│   ├── raw/*.csv                      # source feeds (market/orders/production/shipments/raw_materials)
│   └── processed/{bronze,silver,gold} # Parquet marts (regenerated by Spark)
├── jars/                              # postgres JDBC + spark-sql-kafka jars
├── scripts/
│   ├── spark_jobs/etl_common.py       # REFACTOR: shared helpers + purge_partitions (THE FIX)
│   ├── spark_jobs/{bronze,silver,gold,streaming}_etl.py
│   ├── kafka_producers/*              # market/orders/production/shipments + config.py
│   ├── dashboards/*                   # streaming dashboards (app.py, tenant.py, config.py)
│   ├── ml_jobs/*                      # demand_forecasting, price_prediction, supplier_risk
│   └── data_generators/*              # synthetic data
└── portal/
    ├── Dockerfile  requirements.txt   # REFACTOR: real FastAPI image
    └── app/{main,config,database,security}.py
        ├── routers/{auth,dashboard,factories,upload}.py
        └── templates/{portfolio,demo,login,dashboard}.html
└── n8n/
    ├── workflow_1_realtime_alerting.json
    ├── workflow_2_market_harvest_cron.json
    └── README_n8n.md
```

---

## 9. Run it (out of the box, this zip)
```bash
docker compose up -d --build                 # merges the override automatically

# apply the multi-tenant migration if not already applied
docker compose exec steel-postgres \
  psql -U steel_admin -d steel_db -f /docker-entrypoint-initdb.d/01_multitenant_migration.sql

# (optional) regenerate the medallion layers from raw — already shipped under data/processed
docker compose exec spark-master spark-submit /opt/spark_jobs/bronze_etl.py
docker compose exec spark-master spark-submit /opt/spark_jobs/silver_etl.py
docker compose exec spark-master spark-submit /opt/spark_jobs/gold_etl.py
```
Endpoints: portal http://localhost:8000 (`/`, `/demo`, `/login`, `/app`),
n8n http://localhost:5678 (import the 2 workflow JSONs, add creds, activate),
kafka-ui & airflow on their mapped ports.

---

## 10. The four deliverables (acceptance checklist)
1. **Spark ETL fix** — partition conflict resolved via `purge_partitions`; unified
   tenant-first partitioning; `supplier_scorecard` fixed. ✔
2. **Portal** — Mode A (portfolio + demo + `/api/public/metrics`) and Mode B
   (row-isolated dashboard, `ON CONFLICT` factory registration, validated upload). ✔
3. **n8n** — webhook anomaly alerting + cron market-harvest to Kafka; service added
   to compose. ✔
4. **Defensive handling** — parameterized SQL, retry/backoff, zero-guards, input
   validation (Pydantic + governorate whitelist), env-driven JDBC/broker strings,
   failure-swallowing notify helpers. ✔

---

## 11. Known caveats / things to verify in your environment
- The full Docker stack (Spark + Kafka + Airflow + Postgres) was **not executed**
  end-to-end during the refactor; Python compiles and JSON validates, and all
  configs are mutually consistent, but a first `docker compose up` should be tested.
- The n8n market-harvest HTTP node uses a **placeholder** provider URL — point it at
  a real steel-index API and adjust the Scrub Code node's field mapping.
- `PORTAL_SECRET_KEY` defaults to a dev value — set a real secret in production.
- `data/processed/` is **shipped** here for out-of-the-box demo data; it is fully
  regenerable by re-running the ETL, and the refactor's purge step keeps it clean
  across scheme changes.
- The original Airflow `Dockerfile` is retained for the Airflow services; only the
  portal moved to its own `portal/Dockerfile`.
