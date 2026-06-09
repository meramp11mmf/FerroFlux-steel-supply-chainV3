from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ============================================================
# FERROFLUX — steel_production_etl (TENANT-AWARE)
# ============================================================
# When triggered by the portal after an upload, the trigger conf
# carries {company_id, factory_id}. We pass those into the Spark
# ETL containers as FF_COMPANY / FF_FACTORY env vars, so the ETL
# only reprocesses that factory (fast). A normal @daily run has no
# conf -> FF_* are empty -> full reprocess of all factories.
#
# Templated: {{ dag_run.conf.get('company_id', '') }}
# ============================================================

default_args = {
    'owner': 'nourhan',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# env-var prefix passed to docker exec (empty on scheduled runs)
SCOPE_ENV = (
    "-e FF_COMPANY='{{ dag_run.conf.get('company_id', '') }}' "
    "-e FF_FACTORY='{{ dag_run.conf.get('factory_id', '') }}'"
)

BASE = "/opt/spark/scripts/scripts/spark_jobs"
ML   = "/opt/spark/scripts/scripts/ml_jobs"

with DAG(
    'steel_production_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ferroflux', 'etl', 'multi-tenant'],
) as dag:

    run_silver_etl = BashOperator(
        task_id='run_silver_etl',
        bash_command=f"docker exec {SCOPE_ENV} spark-master python3 {BASE}/silver_etl.py"
    )

    run_fix_etl = BashOperator(
        task_id='run_fix_etl',
        bash_command=f"docker exec {SCOPE_ENV} spark-master python3 {BASE}/fix_production_efficiency.py"
    )

    run_gold_etl = BashOperator(
        task_id='run_gold_etl',
        bash_command=f"docker exec {SCOPE_ENV} spark-master python3 {BASE}/gold_etl.py"
    )

    # ML retraining (tenant-scoped). On scheduled runs retrains on all data;
    # on a factory upload trigger, the ML jobs read FF_* and retrain that
    # factory's models (with global-model fallback when data is sparse).
    run_ml_price = BashOperator(
        task_id='run_ml_price_prediction',
        bash_command=f"docker exec {SCOPE_ENV} spark-master python3 {ML}/price_prediction.py",
    )

    run_ml_demand = BashOperator(
        task_id='run_ml_demand_forecasting',
        bash_command=f"docker exec {SCOPE_ENV} spark-master python3 {ML}/demand_forecasting.py",
    )

    run_ml_risk = BashOperator(
        task_id='run_ml_supplier_risk',
        bash_command=f"docker exec {SCOPE_ENV} spark-master python3 {ML}/supplier_risk.py",
    )

    # Pipeline: Silver -> Fix -> Gold -> (3 ML models in parallel)
    run_silver_etl >> run_fix_etl >> run_gold_etl
    run_gold_etl >> [run_ml_price, run_ml_demand, run_ml_risk]
