"""
FerroFlux — Project Command Center
===================================
Run from WSL inside the project folder:
    python3 manager.py

Requires: docker compose stack is up.
"""
import subprocess
import sys
import os

try:
    import requests
    from requests.auth import HTTPBasicAuth
    _requests_ok = True
except ImportError:
    _requests_ok = False

# ── Config ────────────────────────────────────────────────────────────────────
CONTAINER       = "spark-master"
SPARK_SUBMIT    = "/opt/spark/bin/spark-submit"
SCRIPTS_ROOT    = "/opt/spark/scripts/scripts"   # inside container
AIRFLOW_URL     = "http://localhost:8089/api/v1"
AIRFLOW_USER    = os.getenv("AIRFLOW_USER",    "admin")
AIRFLOW_PASS    = os.getenv("AIRFLOW_PASSWORD", "admin123")
DAG_ID          = "steel_production_etl"

# ── Airflow helper ────────────────────────────────────────────────────────────
class _Airflow:
    def __init__(self):
        self.auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS) if _requests_ok else None

    def _get(self, path):
        if not _requests_ok:
            return None
        try:
            r = requests.get(AIRFLOW_URL + path, auth=self.auth, timeout=5)
            return r.json() if r.ok else None
        except Exception as e:
            print(f"  ⚠  Airflow unreachable: {e}")
            return None

    def _post(self, path, body=None):
        if not _requests_ok:
            return None
        try:
            r = requests.post(AIRFLOW_URL + path, json=body or {}, auth=self.auth, timeout=5)
            return r.json() if r.ok else None
        except Exception as e:
            print(f"  ⚠  Airflow unreachable: {e}")
            return None

    def status(self, dag_id=DAG_ID):
        data = self._get(f"/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date")
        if data and data.get("dag_runs"):
            run = data["dag_runs"][0]
            return run.get("state", "unknown"), run.get("execution_date", "")
        return "unknown", ""

    def trigger(self, dag_id=DAG_ID):
        result = self._post(f"/dags/{dag_id}/dagRuns")
        return result is not None

airflow = _Airflow()

# ── Runners ───────────────────────────────────────────────────────────────────
def _docker_exec(cmd_inside, interactive=True, background=False):
    """Build and run a docker exec command."""
    flag = "-d" if background else ("-it" if interactive else "-i")
    full = f"docker exec {flag} {CONTAINER} {cmd_inside}"
    subprocess.run(full, shell=True)

def spark_submit(script_rel, extra_conf="", env_vars=""):
    """
    Run a PySpark script via spark-submit (ETL + ML jobs all need this).
    script_rel is relative to SCRIPTS_ROOT inside the container.
    """
    script = f"{SCRIPTS_ROOT}/{script_rel}"
    env_part = f"-e {env_vars}" if env_vars else ""
    cmd = (
        f"docker exec -it {env_part} {CONTAINER} "
        f"{SPARK_SUBMIT} "
        f"--conf spark.jars.ivy=/tmp/.ivy2 "
        f"--conf spark.driver.extraJavaOptions=\"-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2\" "
        f"{extra_conf} "
        f"{script}"
    )
    print(f"\n  ▶  spark-submit  {script_rel}")
    subprocess.run(cmd, shell=True)

def run_python(script_rel):
    """Run a plain Python3 script (non-Spark) inside the container."""
    script = f"{SCRIPTS_ROOT}/{script_rel}"
    print(f"\n  ▶  python3  {script_rel}")
    _docker_exec(f"python3 {script}")

def run_streamlit(script_rel):
    """Launch a Streamlit app in the background (port 8501)."""
    script = f"{SCRIPTS_ROOT}/{script_rel}"
    cmd = (
        f"docker exec -d {CONTAINER} "
        f"streamlit run {script} "
        f"--server.port 8501 --server.address 0.0.0.0 "
        f"--server.headless true"
    )
    print(f"\n  ▶  streamlit  {script_rel}  → http://localhost:8501")
    subprocess.run(cmd, shell=True)

def run_streaming():
    """
    Spark Structured Streaming via spark-submit.
    Uses --jars to put kafka-clients on the driver classpath at JVM boot
    (required by KafkaSourceProvider / ByteArraySerializer).
    """
    ivy = "/tmp/ivy2/jars"
    jars = ",".join([
        f"{ivy}/org.apache.spark_spark-sql-kafka-0-10_2.12-3.4.4.jar",
        f"{ivy}/org.apache.kafka_kafka-clients-3.3.2.jar",
        f"{ivy}/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.4.4.jar",
        f"{ivy}/org.lz4_lz4-java-1.8.0.jar",
        f"{ivy}/org.xerial.snappy_snappy-java-1.1.10.5.jar",
        f"{ivy}/org.slf4j_slf4j-api-2.0.6.jar",
        f"{ivy}/org.apache.commons_commons-pool2-2.11.1.jar",
        f"{ivy}/org.postgresql_postgresql-42.7.1.jar",
    ])
    script = f"{SCRIPTS_ROOT}/spark_jobs/streaming_etl.py"
    cmd = (
        f"docker exec -it {CONTAINER} {SPARK_SUBMIT} "
        f"--jars {jars} "
        f"--driver-class-path {jars} "
        f"--conf spark.driver.extraClassPath={jars} "
        f"--conf spark.executor.extraClassPath={jars} "
        f"{script}"
    )
    print(f"\n  ⚡  Launching Spark Streaming → Ctrl+C to stop")
    subprocess.run(cmd, shell=True)

# ── Menu helpers ──────────────────────────────────────────────────────────────
def sep():      print("  " + "─" * 60)
def header(t):  print(f"\n{'═'*70}\n  {t}\n{'═'*70}")
def ask(prompt): return input(f"\n  {prompt}> ").strip().lower()

def pause():
    input("\n  ↩  Press Enter to continue…")

# ── Menus ─────────────────────────────────────────────────────────────────────
def menu_docker():
    header("1 · Docker Lifecycle")
    print("  [1] Start stack          docker compose up -d")
    print("  [2] Stop stack           docker compose down")
    print("  [3] Rebuild & start      docker compose up -d --build")
    print("  [4] Rebuild portal only  docker compose up -d --build steel-portal")
    print("  [0] Back")
    ch = ask("Choose")
    cmds = {
        "1": "docker compose up -d",
        "2": "docker compose down",
        "3": "docker compose up -d --build",
        "4": "docker compose up -d --build steel-portal",
    }
    if ch in cmds:
        subprocess.run(cmds[ch], shell=True, cwd="/home/nourhan/FerroFlux-steel-supply-chainV3")
    pause()

def menu_status():
    header("2 · Status Check")
    print()
    subprocess.run(
        "docker ps --format 'table {{.Names}}\\t{{.Status}}\\t{{.Ports}}'",
        shell=True)
    sep()
    # DB row counts
    counts_sql = (
        "SELECT 'daily_kpis' AS tbl,count(*) FROM analytics.daily_kpis "
        "UNION ALL SELECT 'supplier_scorecard',count(*) FROM analytics.supplier_scorecard "
        "UNION ALL SELECT 'regional_demand',count(*) FROM analytics.regional_demand "
        "UNION ALL SELECT 'production_efficiency',count(*) FROM analytics.production_efficiency "
        "UNION ALL SELECT 'market_clean',count(*) FROM processed_data.market_clean;"
    )
    print("\n  Warehouse row counts:")
    subprocess.run(
        f"docker exec steel-postgres psql -U steel_admin -d steel_db -c \"{counts_sql}\"",
        shell=True)
    pause()

def menu_etl():
    header("3 · ETL Pipelines (spark-submit)")
    print("  [1] Full pipeline     Bronze → Silver → Gold")
    print("  [2] Bronze only")
    print("  [3] Silver only")
    print("  [4] Gold only")
    print("  [0] Back")
    ch = ask("Choose")
    jobs = {
        "2": "spark_jobs/bronze_etl.py",
        "3": "spark_jobs/silver_etl.py",
        "4": "spark_jobs/gold_etl.py",
    }
    if ch == "1":
        for j in ["spark_jobs/bronze_etl.py",
                  "spark_jobs/silver_etl.py",
                  "spark_jobs/gold_etl.py"]:
            spark_submit(j)
    elif ch in jobs:
        spark_submit(jobs[ch])
    pause()

def menu_ml():
    header("4 · AI / ML Models (spark-submit)")
    print("  [1] Price Prediction    (GBT Regressor — 7-day & 30-day forecast)")
    print("  [2] Demand Forecasting  (Random Forest — weekly by region)")
    print("  [3] Supplier Risk       (Risk scoring)")
    print("  [4] Run all three")
    print("  [0] Back")
    ch = ask("Choose")
    jobs = {
        "1": "ml_jobs/price_prediction.py",
        "2": "ml_jobs/demand_forecasting.py",
        "3": "ml_jobs/supplier_risk.py",
    }
    if ch == "4":
        for j in jobs.values():
            spark_submit(j)
    elif ch in jobs:
        spark_submit(jobs[ch])
    pause()

def menu_streaming():
    header("5 · Spark Structured Streaming")
    print("  Consumes Kafka topic  steel_market_prices")
    print("  Press Ctrl+C to stop the stream.\n")
    run_streaming()
    pause()

def menu_kafka():
    header("6 · Kafka Operations")
    print("  [1] Run all producers   (market / orders / production / shipments)")
    print("  [2] Market producer only")
    print("  [3] Orders producer only")
    print("  [4] Production producer only")
    print("  [5] Shipments producer only")
    print("  [6] Verify topics")
    print("  [0] Back")
    ch = ask("Choose")
    producers = {
        "2": "kafka_producers/market_producer.py",
        "3": "kafka_producers/orders_producer.py",
        "4": "kafka_producers/production_producer.py",
        "5": "kafka_producers/shipments_producer.py",
    }
    if ch == "1":
        for p in producers.values():
            run_python(p)
    elif ch in producers:
        run_python(producers[ch])
    elif ch == "6":
        run_python("kafka_producers/verify_topics.py")
    pause()

def menu_dashboard():
    header("7 · Streamlit Dashboard")
    print("  Launches the Streamlit supply-chain dashboard on port 8501.")
    print("  Open:  http://localhost:8501\n")
    confirm = ask("Launch? [y/n]")
    if confirm == "y":
        run_streamlit("dashboards/app.py")
        print("  ✅  Streamlit started in background.")
    pause()

def menu_utilities():
    header("8 · System Utilities")
    print("  [1] Fix production_efficiency table")
    print("  [2] Check bronze parquet data quality")
    print("  [0] Back")
    ch = ask("Choose")
    if ch == "1":
        spark_submit("spark_jobs/fix_production_efficiency.py")
    elif ch == "2":
        subprocess.run(
            f"docker exec spark-master python3 -c \""
            "from pyspark.sql import SparkSession; "
            "spark=SparkSession.builder.appName('check').config('spark.jars.ivy','/tmp/.ivy2').getOrCreate(); "
            "spark.sparkContext.setLogLevel('ERROR'); "
            "df=spark.read.parquet('/opt/spark/data/processed/bronze/production'); "
            "t=df.count(); d=df.select('batch_id').distinct().count(); "
            "print(f'Production bronze: {t} rows, {d} distinct batch_ids, {t-d} dupes'); "
            "spark.stop()"
            "\"",
            shell=True)
    pause()

def menu_airflow():
    header("9 · Airflow — AI Orchestration Agent")
    state, ts = airflow.status()
    print(f"  DAG:         {DAG_ID}")
    print(f"  Last state:  {state}   ({ts[:19] if ts else 'never run'})")
    sep()
    print("  [1] Trigger full ETL pipeline via Airflow")
    print("  [2] Refresh status")
    print("  [0] Back")
    ch = ask("Choose")
    if ch == "1":
        if airflow.trigger():
            print("  ✅  DAG triggered — watch progress at http://localhost:8089")
        else:
            print("  ❌  Trigger failed. Is Airflow up? (http://localhost:8089)")
    elif ch == "2":
        state, ts = airflow.status()
        print(f"  Current state: {state}   ({ts[:19] if ts else '—'})")
    pause()

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    os.chdir("/home/nourhan/FerroFlux-steel-supply-chainV3")

    menus = {
        "1": menu_docker,
        "2": menu_status,
        "3": menu_etl,
        "4": menu_ml,
        "5": menu_streaming,
        "6": menu_kafka,
        "7": menu_dashboard,
        "8": menu_utilities,
        "9": menu_airflow,
    }

    while True:
        print("\n" + "═" * 70)
        print("  🔥  FerroFlux · Steel Supply-Chain Command Center")
        print("═" * 70)
        print("  [1] 🐳 Docker Lifecycle      [2] 🔍 Status & Row Counts")
        print("  [3] 🔄 ETL Pipelines         [4] 🤖 AI / ML Models")
        print("  [5] ⚡ Spark Streaming        [6] 📨 Kafka Operations")
        print("  [7] 📊 Streamlit Dashboard    [8] 🔧 System Utilities")
        print("  [9] 🧠 Airflow AI Agent       [0] 🚪 Exit")
        print("═" * 70)

        ch = ask("Select")
        if ch == "0":
            print("\n  Goodbye!\n")
            sys.exit(0)
        elif ch in menus:
            menus[ch]()
        else:
            print("  ❌  Invalid option — try again.")

if __name__ == "__main__":
    main()
