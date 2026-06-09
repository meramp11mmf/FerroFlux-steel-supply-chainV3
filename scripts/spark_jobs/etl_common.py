# ============================================================
# etl_common.py  —  shared helpers for the Medallion ETL jobs
# ============================================================
# Centralises the things that were previously copy-pasted (and
# sometimes wrong) across bronze/silver/gold:
#
#   * DYNAMIC connection strings built from env vars (Deliverable 4)
#       -> no more hard-coded jdbc:postgresql://steel-postgres:...
#   * A STANDARDISED partitioned writer that PURGES legacy layouts
#     before writing, which is the actual fix for:
#       java.lang.AssertionError: assertion failed:
#       Conflicting partition column names detected
#   * Tenant-safe PostgreSQL upsert (delete-by-tenant, then append)
#   * A defensive n8n webhook emitter for anomaly alerting
#
# Drop this file in: scripts/spark_jobs/etl_common.py
# (same folder as silver_etl.py / gold_etl.py, so `import etl_common`
#  works because Python puts the script's dir on sys.path).
# ============================================================
import os
import shutil
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [etl_common] %(message)s")
log = logging.getLogger("etl_common")


# ------------------------------------------------------------
# 1. DYNAMIC CONNECTION CONFIG (env-driven, Docker-network safe)
# ------------------------------------------------------------
def pg_conf() -> dict:
    """
    Build the PostgreSQL connection parameters from the environment so
    the same image works in any compose/network without code edits.
    Falls back to the project defaults if an env var is missing.
    """
    return {
        "host": os.getenv("PG_HOST", "steel-postgres"),
        "port": os.getenv("PG_PORT", "5432"),
        "db":   os.getenv("PG_DB",   "steel_db"),
        "user": os.getenv("PG_USER", os.getenv("POSTGRES_USER", "steel_admin")),
        "password": os.getenv("PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "steel_pass_2024")),
        "driver": "org.postgresql.Driver",
    }


def jdbc_url(conf: dict = None) -> str:
    """Dynamic JDBC string -> jdbc:postgresql://<host>:<port>/<db>."""
    c = conf or pg_conf()
    return f"jdbc:postgresql://{c['host']}:{c['port']}/{c['db']}"


def jdbc_props(conf: dict = None) -> dict:
    """Spark JDBC properties dict (user/password/driver)."""
    c = conf or pg_conf()
    return {"user": c["user"], "password": c["password"], "driver": c["driver"]}


# ------------------------------------------------------------
# 2. TENANT SCOPE (driven by Airflow trigger conf -> env vars)
# ------------------------------------------------------------
def tenant_scope():
    """Return (company_id, factory_id) requested for this run, '' if full run."""
    return (os.getenv("FF_COMPANY", "").strip(),
            os.getenv("FF_FACTORY", "").strip())


def apply_scope(df):
    """Filter a dataframe to the requested tenant, if any."""
    company, factory = tenant_scope()
    if company and "company_id" in df.columns:
        df = df.filter(df.company_id == company)
    if factory and "factory_id" in df.columns:
        df = df.filter(df.factory_id == factory)
    return df


# ------------------------------------------------------------
# 3. PARTITION PURGE  (THE FIX for "Conflicting partition column names")
# ------------------------------------------------------------
def purge_partitions(path, partition_cols, company=None, factory=None):
    """
    Guarantee a UNIFORM partition layout under `path` before writing.

    The crash happened because dynamic-overwrite leaves stale directories
    from a previous run that used a DIFFERENT partition scheme (e.g. a
    legacy `region=.../` sitting next to the new
    `company_id=.../factory_id=.../region=.../`). When Gold then reads the
    folder, Spark infers two different partition-column sets and asserts.

    Strategy (in order):
      A. Remove any top-level partition dir whose key != the expected first
         partition column  -> kills the legacy single-leaf layout.
      B. Tenant-scoped run: remove ONLY this tenant's subtree so other
         factories survive, and the re-run rewrites a clean subtree.
      C. Full run (no tenant scope): remove the whole table dir for a
         100%-uniform rebuild.

    Runs on the driver against the local shared volume (Spark uses
    LocalFileSystem here), so plain os/shutil is correct and fast.
    """
    if not partition_cols:
        return
    expected_first = partition_cols[0]

    if not os.path.isdir(path):
        return

    # --- A. drop legacy / conflicting top-level partition dirs ---
    try:
        for child in os.listdir(path):
            child_path = os.path.join(path, child)
            if not os.path.isdir(child_path) or "=" not in child:
                continue
            key = child.split("=", 1)[0]
            if key != expected_first:
                log.info(f"purge: removing legacy partition dir '{child}' under {path}")
                shutil.rmtree(child_path, ignore_errors=True)
    except Exception as e:
        log.warning(f"purge step A skipped for {path}: {e}")

    tenant_partitioned = partition_cols[:2] == ["company_id", "factory_id"]

    # --- C. full run: wipe everything for a uniform rebuild ---
    if not company and tenant_partitioned:
        log.info(f"purge: full rebuild -> wiping {path}")
        shutil.rmtree(path, ignore_errors=True)
        return
    if not tenant_partitioned:
        # global table (e.g. partitioned by 'year'): always full rebuild
        log.info(f"purge: global table rebuild -> wiping {path}")
        shutil.rmtree(path, ignore_errors=True)
        return

    # --- B. tenant-scoped run: drop only this tenant's subtree ---
    sub = os.path.join(path, f"company_id={company}")
    if factory:
        sub = os.path.join(sub, f"factory_id={factory}")
    if os.path.isdir(sub):
        log.info(f"purge: tenant rebuild -> removing {sub}")
        shutil.rmtree(sub, ignore_errors=True)


def write_partitioned(df, path, partition_cols, company=None, factory=None):
    """
    Purge-then-write a parquet table with the STANDARDISED multi-level
    partition hierarchy. Always coalesce the partition columns to the
    front so the on-disk layout is deterministic.
    """
    missing = [c for c in partition_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"write_partitioned: dataframe for {path} is missing partition "
            f"columns {missing}. Present columns: {df.columns}")

    purge_partitions(path, partition_cols, company=company, factory=factory)
    (df.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(*partition_cols)
        .parquet(path))
    log.info(f"wrote {path} partitioned by {partition_cols}")


# ------------------------------------------------------------
# 4. TENANT-SAFE POSTGRES WRITE (delete-by-tenant, then append)
# ------------------------------------------------------------
def save_pg_tenant(df, pg_table, conf=None):
    """
    Idempotent PG write that preserves other tenants:
      1. Align the DataFrame to the target table's actual column set:
         - Inspect information_schema.columns for the exact ordered column list.
         - Derive any missing column the table needs but the df lacks
           (currently: derive 'year' from 'date' when the table has 'year').
         - Drop df columns the table does not have (e.g. transient 'year'
           used only for parquet partitionBy).
         - Re-select in the exact table column order so JDBC INSERT is clean.
      2. DELETE the tenant's existing rows (parameterised, no SQL injection).
      3. Append the aligned rows.
    Composite PKs (company_id, factory_id, <key>) make this collision-free.
    """
    import psycopg2
    c = conf or pg_conf()

    # ---- 1. align df to actual table schema --------------------------------
    try:
        from pyspark.sql import functions as _F
        from pyspark.sql.types import BooleanType as _BoolT
        _sch, _tbl = pg_table.split('.') if '.' in pg_table else ('public', pg_table)
        _conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["db"],
                                 user=c["user"], password=c["password"])
        _cur = _conn.cursor()
        # column list with types
        _cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (_sch, _tbl))
        tbl_meta = [(r[0], r[1]) for r in _cur.fetchall()]
        # primary key columns (for deduplication)
        _cur.execute(
            "SELECT kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema "
            "WHERE tc.table_schema=%s AND tc.table_name=%s AND tc.constraint_type='PRIMARY KEY' "
            "ORDER BY kcu.ordinal_position",
            (_sch, _tbl))
        pk_cols = [r[0] for r in _cur.fetchall()]
        _cur.close(); _conn.close()
        if tbl_meta:
            tbl_cols = [m[0] for m in tbl_meta]
            tbl_types = {m[0]: m[1] for m in tbl_meta}
            df_set = set(df.columns)
            df_schema = {f.name: f.dataType for f in df.schema.fields}
            # derive columns the table needs but the df does not yet have
            if "year" in tbl_cols and "year" not in df_set and "date" in df_set:
                df = df.withColumn("year", _F.year(_F.col("date")))
                df_set.add("year")
                df_schema["year"] = df.schema["year"].dataType
            # cast boolean df columns to int when the pg table expects integer
            for col_name in list(df_set):
                if (col_name in tbl_types
                        and tbl_types[col_name] == "integer"
                        and isinstance(df_schema.get(col_name), _BoolT)):
                    df = df.withColumn(col_name, _F.col(col_name).cast("integer"))
            # select only table columns that exist in the df, in table order
            keep = [col for col in tbl_cols if col in df_set]
            if keep:
                df = df.select(*keep)
        # deduplicate on PK cols to prevent unique-constraint violations
        valid_pk = [p for p in pk_cols if p in df.columns]
        if valid_pk:
            df = df.dropDuplicates(valid_pk)
    except Exception as _e:
        log.warning(f"{pg_table}: schema alignment skipped ({_e})")
    # ---- end schema alignment -----------------------------------------------

    if "company_id" not in df.columns or "factory_id" not in df.columns:
        log.warning(f"{pg_table}: no tenant columns; appending without pre-delete")
    else:
        try:
            pairs = [(r["company_id"], r["factory_id"])
                     for r in df.select("company_id", "factory_id").distinct().collect()]
            if pairs:
                conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["db"],
                                        user=c["user"], password=c["password"])
                cur = conn.cursor()
                for company, factory in pairs:
                    cur.execute(
                        f"DELETE FROM {pg_table} WHERE company_id = %s AND factory_id = %s",
                        (company, factory))
                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            log.warning(f"{pg_table}: tenant pre-delete skipped: {e}")

    df.write.mode("append").jdbc(jdbc_url(c), pg_table, properties=jdbc_props(c))
    log.info(f"appended fresh rows into {pg_table}")


# ------------------------------------------------------------
# 5. n8n ANOMALY ALERTING (defensive, never breaks the pipeline)
# ------------------------------------------------------------
def notify_n8n(event_type, payload):
    """
    POST an anomaly payload to the n8n webhook. Failures are logged and
    swallowed so a notification outage can never fail the ETL.
    Configure with N8N_WEBHOOK_URL (e.g.
      http://n8n:5678/webhook/steel-alert ).
    """
    url = os.getenv("N8N_WEBHOOK_URL", "").strip()
    if not url:
        return {"sent": False, "reason": "N8N_WEBHOOK_URL not set"}
    try:
        import requests
        body = {"event_type": event_type, "payload": payload}
        resp = requests.post(url, json=body, timeout=8)
        ok = resp.status_code in (200, 201, 202, 204)
        if not ok:
            log.warning(f"n8n alert HTTP {resp.status_code}: {resp.text[:200]}")
        return {"sent": ok, "status": resp.status_code}
    except Exception as e:
        log.warning(f"n8n alert failed: {e}")
        return {"sent": False, "reason": str(e)}


def emit_anomalies_from_df(df, event_type, key_cols, max_rows=200):
    """
    Collect rows from an 'anomaly' dataframe (already filtered) and fan
    them out to n8n one event at a time. Capped at `max_rows` to protect
    the driver. Each event carries the tenant so n8n can route per-tenant.
    """
    if os.getenv("N8N_WEBHOOK_URL", "").strip() == "":
        return 0
    try:
        rows = df.select(*key_cols).limit(max_rows).collect()
    except Exception as e:
        log.warning(f"emit_anomalies_from_df collect failed: {e}")
        return 0
    sent = 0
    for r in rows:
        payload = {k: (r[k] if k in r.__fields__ else None) for k in key_cols}
        res = notify_n8n(event_type, payload)
        sent += 1 if res.get("sent") else 0
    log.info(f"emitted {sent}/{len(rows)} {event_type} alerts to n8n")
    return sent


def safe_pct(numerator, denominator):
    """Division guard for log/print stats so empty tenants don't crash."""
    return (numerator / denominator * 100) if denominator else 0.0
