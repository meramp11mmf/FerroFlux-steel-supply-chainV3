# FerroFlux V3 — Refactor Notes

This package refactors the steel supply-chain platform across all four
deliverable areas. Every file below mirrors its place in the original project
tree, so applying the refactor is mostly a copy-over plus a DB migration and a
portal rebuild.

```
steel-refactor/
├── scripts/spark_jobs/
│   ├── etl_common.py          (NEW — shared ETL helpers, the partition fix)
│   ├── silver_etl.py          (refactored)
│   └── gold_etl.py            (refactored)
├── portal/
│   ├── Dockerfile             (NEW — real FastAPI image)
│   ├── requirements.txt       (NEW)
│   └── app/
│       ├── main.py            (NEW — Mode A + Mode B app)
│       ├── config.py          (NEW)
│       ├── database.py        (NEW)
│       ├── security.py        (NEW — tokens + tenant isolation)
│       ├── routers/{auth,dashboard,factories,upload}.py
│       └── templates/{portfolio,demo,login,dashboard}.html  (NEW)
├── n8n/
│   ├── workflow_1_realtime_alerting.json   (NEW)
│   ├── workflow_2_market_harvest_cron.json (NEW)
│   └── README_n8n.md
├── docker-compose.override.yml  (NEW)
└── REFACTOR_NOTES.md            (this file)
```

---

## 1 · Spark ETL — the partitioning crash

### Root cause
`silver_etl.py` and `gold_etl.py` wrote every table with
`.mode("overwrite").option("partitionOverwriteMode","dynamic")`. **Dynamic**
overwrite only replaces the partition directories that exist in the *new*
DataFrame; it never deletes stale directories left by an earlier run that used a
**different** partition scheme. So a legacy layout like:

```
orders_clean/region=Delta/...
```

would coexist with the new multi-tenant layout:

```
orders_clean/company_id=EZZ/factory_id=EZZ_ALEX/region=Delta/...
```

Two different partition *depths* under one path. When Gold then did a bare
`spark.read.parquet("…/orders_clean")`, Spark walked both layouts and threw:

```
java.lang.AssertionError: Conflicting partition column names detected
```

### Fix
A new shared module, **`scripts/spark_jobs/etl_common.py`**, centralises the
write path. The key function is `purge_partitions()`, called *before every
write*:

- On a **global / full** run it wipes the table directory entirely, so no stale
  scheme can survive.
- On a **tenant-scoped** run it removes only that tenant's subtree (plus any
  legacy top-level directory whose first key doesn't match the expected
  `company_id` partition column).

`write_partitioned()` then writes the standardized layout. Because the old dirs
are gone first, the partition depth is always uniform and the Gold read never
sees a conflict — latent today, fatal the moment the scheme changes, now fixed
permanently.

### Unified partition strategy
Every Silver/Gold table is now partitioned tenant-first:

| Table | Partition columns |
|-------|-------------------|
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

### Other Spark bugs fixed
- **`supplier_scorecard` AnalysisException** — the original `.select(...)`
  dropped `company_id` / `factory_id` and then tried to
  `.partitionBy("company_id","factory_id","material_type")`. The tenant columns
  are now kept in the projection.
- **Divide-by-zero in stats** — all ratio/percentage prints and `gross_margin`
  are guarded (`safe_pct`, zero-revenue guards) so an empty tenant slice no
  longer crashes the job.
- **Hard-coded JDBC** — connection strings now come from env
  (`PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD`, defaulting to
  `steel-postgres / 5432 / steel_db / steel_admin / steel_pass_2024`).

> All original business logic is preserved — FX conversions, governorate→region
> mapping, efficiency/delay scoring — only the write path and guards changed.

---

## 2 · The steel-portal (FastAPI)

The zip only contained `portal/app/routers/{auth,upload}.py` — no `main.py`,
`database.py`, templates, or Dockerfile. The portal is now complete and runs in
two modes from one service:

**Mode A (public)**
- `/` portfolio + architecture/topology story
- `/demo` live demo dashboard (Chart.js) reading `/api/public/metrics`
- no signup required, no tenant data exposed (synthetic series only)

**Mode B (secure, multi-tenant)**
- `/login` sign-in + self-service registration
- `/app` tenant SPA (overview, production, regional, suppliers, factories, upload)
- `/api/auth/*`, `/api/dashboard/*`, `/api/factories`, `/api/upload/*`

### Row-level isolation (the important part)
`app/security.py` issues an HMAC-SHA256 **signed token** carrying the tenant's
`company_id` / `factory_id`. Every authenticated endpoint depends on
`current_tenant`, and **all** queries apply `tenant.where()` — a parameterised
`company_id = %s [AND factory_id = %s]` fragment derived *only* from the verified
token, never from request input. There is no code path where a client can widen
its own scope.

### Factory registration
`/api/factories` (and the registration flow) use the required idempotent
**`ON CONFLICT (factory_id) DO UPDATE`** upsert, with `company_id` taken from the
token so an admin can't create units under another company.

### Password-mismatch fix
`config.py` builds `DATABASE_URL` from parts and defaults the password to
`steel_pass_2024` (matching Postgres / the ETL), resolving the
`steel_pass` mismatch in the base compose.

---

## 3 · n8n automation

See `n8n/README_n8n.md` for import + credential steps. Two workflows:

1. **Real-time alerting** — webhook `steel-alert` receives `{event_type, payload}`
   from the Spark jobs (price spike, sub-70% efficiency, factory onboarding) and
   routes per tenant to Slack / Telegram / email.
2. **Market harvest cron** — daily 06:00 fetch of steel indices → scrub →
   publish to Kafka topic `steel_market_prices` on `kafka:29092` (consumed by
   `streaming_etl.py`).

`n8n` was named in the spec but missing from compose — it's added in the
override below.

---

## 4 · Defensive error handling

Applied throughout: existence checks before reads, zero-guards on every ratio,
parameterised SQL everywhere (no string interpolation of user input),
connection retry/backoff in `database.py`, `fetch_all/fetch_one` returning `[]`
on error rather than raising, env-driven JDBC/broker/Airflow strings, and
input validation via Pydantic (`EmailStr`, length `constr`) plus an explicit
governorate whitelist. The n8n→Spark notify helpers swallow failures so a
down webhook never breaks an ETL run.

---

## How to apply

```bash
# from the project root (where docker-compose.yml lives)

# 1. copy refactored code over the originals
cp -r steel-refactor/scripts/spark_jobs/*.py   scripts/spark_jobs/
cp -r steel-refactor/portal/*                  portal/
cp     steel-refactor/docker-compose.override.yml .
mkdir -p n8n && cp -r steel-refactor/n8n/* n8n/

# 2. ensure the tenant schema exists (composite PKs, tenants.* tables)
#    run the existing migration if you haven't already:
docker compose exec steel-postgres \
  psql -U steel_admin -d steel_db -f /docker-entrypoint-initdb.d/01_multitenant_migration.sql

# 3. (optional) set a real token-signing secret
export PORTAL_SECRET_KEY="$(openssl rand -hex 32)"

# 4. rebuild + start (compose auto-merges the override)
docker compose up -d --build

# 5. re-run the pipeline to lay down the clean, uniform partitions
docker compose exec spark-master \
  spark-submit /opt/spark_jobs/silver_etl.py
docker compose exec spark-master \
  spark-submit /opt/spark_jobs/gold_etl.py
```

Then:
- Portal → http://localhost:8000  (portfolio) · `/demo` · `/login`
- n8n → http://localhost:5678  (import the two workflow JSONs, add credentials, activate)

Demo login: **admin / admin123**, or "enter as guest" on the login page.
