import subprocess
import sys
import requests
import os
from requests.auth import HTTPBasicAuth

# ── Airflow Manager Class (The Smart Brain) ──────────────────────────────────
class AirflowManager:
    def __init__(self):
        self.base_url = "http://localhost:8089/api/v1"
        self.user = os.getenv("AIRFLOW_USER", "admin")
        # Require env var — no insecure default
        self.password = os.getenv("AIRFLOW_PASSWORD", "")
        self.auth = HTTPBasicAuth(self.user, self.password)

    def trigger_dag(self, dag_id):
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        try:
            response = requests.post(url, json={}, auth=self.auth)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Connection Error: {e}")
            return False

    def get_last_run_status(self, dag_id):
        url = f"{self.base_url}/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date"
        try:
            response = requests.get(url, auth=self.auth)
            if response.status_code == 200:
                return response.json()['dag_runs'][0]['state']
        except:
            return "unknown"
        return "unknown"

# ── Initialization ──────────────────────────────────────────────────────────
airflow = AirflowManager()
PYTHONPATH = "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
BASE_PATH = "/opt/spark/scripts"

# ── Helper Functions ────────────────────────────────────────────────────────
def run_cmd(script_rel_path, container="spark-master"):
    full_path = f"{BASE_PATH}/{script_rel_path}"
    cmd = f"docker exec -d -e PYTHONPATH={PYTHONPATH} {container} python3 {full_path}"
    print(f"\n🚀 [Executing Background]: {script_rel_path}")
    subprocess.run(cmd, shell=True)

def run_blocking_cmd(script_rel_path, container="spark-master"):
    full_path = f"{BASE_PATH}/{script_rel_path}"
    cmd = f"docker exec -it -e PYTHONPATH={PYTHONPATH} {container} python3 {full_path}"
    print(f"\n🚀 [Executing Blocking]: {script_rel_path}")
    subprocess.run(cmd, shell=True)

def run_streaming_job(container="spark-master"):
    """
    Run Spark Streaming via spark-submit with an explicit driver classpath.
    This is REQUIRED for Kafka integration: KafkaSourceProvider loads
    ByteArraySerializer at stream-definition time (driver side), before
    --packages jars are placed on the classpath by Ivy.
    Using --driver-class-path ensures kafka-clients is available from JVM boot.
    """
    ivy_jars = "/tmp/ivy2/jars"  # Actual Spark Ivy cache path (no dot prefix)

    jars_list = [
        f"{ivy_jars}/org.apache.spark_spark-sql-kafka-0-10_2.12-3.4.4.jar",
        f"{ivy_jars}/org.apache.kafka_kafka-clients-3.3.2.jar",
        f"{ivy_jars}/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.4.4.jar",
        f"{ivy_jars}/org.lz4_lz4-java-1.8.0.jar",
        f"{ivy_jars}/org.xerial.snappy_snappy-java-1.1.10.5.jar",
        f"{ivy_jars}/org.slf4j_slf4j-api-2.0.6.jar",
        f"{ivy_jars}/org.apache.commons_commons-pool2-2.11.1.jar",
        f"{ivy_jars}/org.postgresql_postgresql-42.7.1.jar",
    ]
    jars_str = ",".join(jars_list)

    cmd = (
        f"docker exec -it {container} /opt/spark/bin/spark-submit "
        f"--jars {jars_str} "
        f"--driver-class-path {jars_str} "                          # fixes ByteArraySerializer
        f"--conf spark.driver.extraClassPath={jars_str} "           # belt-and-suspenders
        f"--conf spark.executor.extraClassPath={jars_str} "         # executor side
        "/opt/spark/scripts/scripts/spark_jobs/streaming_etl.py"
    )

    print(f"\n⚡ [Launching Spark Streaming Job - Strict Classpath Mode]...")
    subprocess.run(cmd, shell=True)

def run_streamlit(script_rel_path, container="spark-master"):
    full_path = f"{BASE_PATH}/{script_rel_path}"
    cmd = (
        f"docker exec -d -e PYTHONPATH={PYTHONPATH} {container} "
        f"streamlit run {full_path} --server.port 8501 --server.address 0.0.0.0"
    )
    print(f"\n🚀 [Launching Dashboard]: {script_rel_path}")
    subprocess.run(cmd, shell=True)

def show_menu():
    print("\n" + "=" * 80)
    print("🌟 STEEL SUPPLY CHAIN - FULL COMMAND CENTER (ADMIN MODE) 🌟")
    print("=" * 80)
    print(" [1] 🐳 Docker Lifecycle | [2] 🔍 Status Check")
    print(" [3] 🔄 ETL Pipelines    | [4] 🤖 AI/ML Models")
    print(" [5] ⚡ Spark Streaming   | [6] 📨 Kafka Operations")
    print(" [7] 📊 Dashboard        | [8] 🔧 System Utilities")
    print(" [9] 🧠 AI Agent (Airflow) | [0] 🚪 Exit")
    print("=" * 80)

def main():
    while True:
        show_menu()
        choice = input("👉 Select Category: ").strip()

        if choice == '1':
            sub = input("   [Up/Down/Build]? ").lower()
            if 'up' in sub:
                subprocess.run("docker compose up -d", shell=True)
            elif 'down' in sub:
                subprocess.run("docker compose down", shell=True)
            elif 'build' in sub:
                subprocess.run("docker compose up -d --build", shell=True)

        elif choice == '2':
            subprocess.run("docker ps --format 'table {{.Names}}\t{{.Status}}'", shell=True)

        elif choice == '3':
            print(" [3.1] Full Pipeline | [3.2] Bronze | [3.3] Silver | [3.4] Gold")
            sub = input("   Option: ")
            jobs = {'3.2': 'bronze', '3.3': 'silver', '3.4': 'gold'}
            if sub == '3.1':
                for s in ["bronze", "silver", "gold"]:
                    run_blocking_cmd(f"scripts/spark_jobs/{s}_etl.py")
            elif sub in jobs:
                run_blocking_cmd(f"scripts/spark_jobs/{jobs[sub]}_etl.py")

        elif choice == '4':
            print(" [4.1] Price Prediction | [4.2] Demand Forecast | [4.3] Supplier Risk")
            sub = input("   Option: ")
            if sub == '4.1':
                run_blocking_cmd("scripts/ml_jobs/price_prediction.py")
            elif sub == '4.2':
                run_blocking_cmd("scripts/ml_jobs/demand_forecasting.py")
            elif sub == '4.3':
                run_blocking_cmd("scripts/ml_jobs/supplier_risk.py")

        elif choice == '5':
            run_streaming_job()  # spark-submit with explicit driver classpath — fixes ByteArraySerializer

        elif choice == '6':
            print(" [6.0] Run All Producers | [6.1] Verify Topics")
            sub = input("   Option: ")
            if sub == '6.0':
                for p in ["market", "orders", "production", "shipments"]:
                    run_cmd(f"scripts/kafka_producers/{p}_producer.py")
            elif sub == '6.1':
                run_blocking_cmd("scripts/kafka_producers/verify_topics.py")

        elif choice == '7':
            run_streamlit("scripts/dashboards/app.py")

        elif choice == '8':
            run_blocking_cmd("scripts/spark_jobs/fix_production_efficiency.py")

        elif choice == '9':
            print(f"\n🧠 [AI Agent] Current ETL Status: {airflow.get_last_run_status('steel_production_etl')}")
            act = input("   [T]rigger ETL Pipeline | [S]tatus Check: ").lower()
            if act == 't':
                if airflow.trigger_dag('steel_production_etl'):
                    print("✅ Pipeline Triggered via Airflow!")
            elif act == 's':
                print(f"📡 Airflow Reports: {airflow.get_last_run_status('steel_production_etl')}")

        elif choice == '0':
            sys.exit(0)

        else:
            print("❌ Invalid selection.")

if __name__ == "__main__":
    main()
