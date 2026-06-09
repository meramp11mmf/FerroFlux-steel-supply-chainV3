# ============================================================
# BRONZE LAYER ETL - Full Version (Raw CSV to Parquet + PostgreSQL)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os

print("=" * 60)
print("BRONZE LAYER ETL - Starting...")
print(f"Time: {datetime.now()}")
print("=" * 60)

# ------ Spark Session Setup ------
spark = (SparkSession.builder
    .appName("Steel_Supply_Chain_ETL")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2")
    .config("spark.jars", "/opt/spark/scripts/jars/postgresql-42.7.1.jar,/opt/spark/scripts/jars/spark-sql-kafka-0-10_2.12-3.4.4.jar")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.enable", "false")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .config("spark.hadoop.dfs.permissions.enabled", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ------ Paths ------
RAW_PATH = "/opt/spark/data/raw"
BRONZE_PATH = "/opt/spark/data/processed/bronze"

# ------ PostgreSQL Config ------
PG_URL = "jdbc:postgresql://steel-postgres:5432/steel_db"
PG_PROPS = {
    "user": "steel_admin",
    "password": "steel_pass_2024",
    "driver": "org.postgresql.Driver"
}

# ── TENANT TAGGING (multi-tenant) ────────────────────────────
# Bronze CSV data is the EZZ demo dataset by default. Real factory
# uploads come through the portal (which tags rows itself), so the
# bronze batch job always tags its CSV rows as EZZ / EZZ_DEMO.
import os as _os
FF_COMPANY = _os.getenv("FF_COMPANY", "EZZ").strip() or "EZZ"
FF_FACTORY = _os.getenv("FF_FACTORY", "EZZ_DEMO").strip() or "EZZ_DEMO"

def _tag_tenant(df):
    return (df.withColumn("company_id", F.lit(FF_COMPANY))
              .withColumn("factory_id", F.lit(FF_FACTORY)))
# ─────────────────────────────────────────────────────────────


def load_csv(filename, name):
    path = f"{RAW_PATH}/{filename}"
    print(f"\nLoading {name} from {path}...")
    
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    df = df.cache()
    count = df.count()
    cols = len(df.columns)
    print(f"   Loaded: {count:,} rows x {cols} columns")

    null_counts = df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
    null_row = null_counts.collect()[0]
    has_nulls = False
    for c in df.columns:
        if null_row[c] > 0:
            print(f"   WARNING {c}: {null_row[c]} nulls")
            has_nulls = True
    if not has_nulls:
        print(f"   No nulls found!")

    return df, count

def save_bronze(df, parquet_name, pg_table, count, partition_by=None):
    df_bronze = _tag_tenant(df).withColumn("loaded_at", F.current_timestamp())

    parquet_path = f"{BRONZE_PATH}/{parquet_name}"
    writer = df_bronze.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(parquet_path)
    print(f"   [OK] Parquet saved: {parquet_path}")

    df_bronze.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", PG_URL) \
        .option("dbtable", pg_table) \
        .option("user", PG_PROPS["user"]) \
        .option("password", PG_PROPS["password"]) \
        .option("driver", PG_PROPS["driver"]) \
        .option("truncate", "true") \
        .option("batchsize", "1000") \
        .save()
    
    print(f"   [OK] PostgreSQL loaded: {pg_table} ({count:,} rows)")
    df.unpersist()

# ============================================================
# 1. MARKET DATA
# ============================================================
df_market, cnt_market = load_csv("market_data.csv", "Market Data")
save_bronze(df_market, "market_prices", "raw_data.market_prices", cnt_market, partition_by="date")

# ============================================================
# 2. PRODUCTION DATA
# ============================================================
df_prod, cnt_prod = load_csv("production.csv", "Production Data")
save_bronze(df_prod, "production", "raw_data.production", cnt_prod, partition_by="facility")

# ============================================================
# 3. ORDERS DATA
# ============================================================
df_orders, cnt_orders = load_csv("orders.csv", "Orders Data")
save_bronze(df_orders, "orders", "raw_data.orders", cnt_orders, partition_by="delivery_governorate")

# ============================================================
# 4. SHIPMENTS DATA
# ============================================================
df_ship, cnt_ship = load_csv("shipments.csv", "Shipments Data")
save_bronze(df_ship, "shipments", "raw_data.shipments", cnt_ship, partition_by="transport_mode")

# ============================================================
# 5. RAW MATERIALS DATA
# ============================================================
df_rawmat, cnt_rawmat = load_csv("raw_materials.csv", "Raw Materials Data")
save_bronze(df_rawmat, "raw_materials", "raw_data.raw_materials", cnt_rawmat)

# ============================================================
# SUMMARY
# ============================================================
total = cnt_market + cnt_prod + cnt_orders + cnt_ship + cnt_rawmat
print("\n" + "=" * 60)
print("BRONZE LAYER ETL - COMPLETE!")
print(f"Total records processed: {total:,}")
print("=" * 60)

spark.stop()