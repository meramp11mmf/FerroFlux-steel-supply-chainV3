# ============================================================
# SPARK STRUCTURED STREAMING - 4 Kafka Topics to PostgreSQL
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time
from datetime import datetime

print("=" * 60)
print("SPARK STREAMING ETL - Starting...")
print(f"Time: {datetime.now()}")
print("=" * 60)
# في ملف streaming_etl.py
jars_path = "/opt/spark/jars"

kafka_jar = "spark-sql-kafka-0-10_2.12-3.4.4.jar"
postgres_jar = "postgresql-42.7.1.jar"
spark = (SparkSession.builder
    .appName("Steel_Supply_Chain_ETL")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,org.postgresql:postgresql:42.7.1")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.enable", "false")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .config("spark.hadoop.dfs.permissions.enabled", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp/ivy2 -Divy.home=/tmp/ivy2")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

KAFKA_BROKER = "kafka:29092"

PG_URL = "jdbc:postgresql://steel-postgres:5432/steel_db"
PG_PROPS = {
    "user": "steel_admin",
    "password": "steel_pass_2024",
    "driver": "org.postgresql.Driver"
}

# ============================================================
# SCHEMA DEFINITIONS (match Kafka JSON messages)
# ============================================================

market_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("producer", StringType()),
    StructField("cycle", IntegerType()),
    StructField("date", StringType()),
    StructField("steel_price_egypt_egp", StringType()),
    StructField("iron_ore_price_usd", StringType()),
    StructField("scrap_price_usd", StringType()),
    StructField("usd_egp_rate", StringType()),
    StructField("natural_gas_price_usd", StringType()),
    StructField("brent_oil_usd", StringType()),
    StructField("electricity_price_egp_kwh", StringType()),
    StructField("seasonality_index", StringType()),
    StructField("is_ramadan", StringType())
])

orders_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("producer", StringType()),
    StructField("cycle", IntegerType()),
    StructField("order_id", StringType()),
    StructField("order_date", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_type", StringType()),
    StructField("product_type", StringType()),
    StructField("rebar_size_mm", StringType()),
    StructField("quantity_tons", StringType()),
    StructField("price_per_ton_egp", StringType()),
    StructField("total_value_egp", StringType()),
    StructField("delivery_governorate", StringType()),
    StructField("status", StringType())
])

production_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("producer", StringType()),
    StructField("cycle", IntegerType()),
    StructField("batch_id", StringType()),
    StructField("date", StringType()),
    StructField("facility", StringType()),
    StructField("production_line", StringType()),
    StructField("line_type", StringType()),
    StructField("product_type", StringType()),
    StructField("shift", StringType()),
    StructField("planned_tons", StringType()),
    StructField("actual_tons", StringType()),
    StructField("efficiency_pct", StringType()),
    StructField("quality_score", StringType()),
    StructField("status", StringType())
])

shipments_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("producer", StringType()),
    StructField("cycle", IntegerType()),
    StructField("shipment_id", StringType()),
    StructField("order_id", StringType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
    StructField("distance_km", StringType()),
    StructField("transport_mode", StringType()),
    StructField("weight_tons", StringType()),
    StructField("transport_cost_egp", StringType()),
    StructField("status", StringType()),
    StructField("delay_days", StringType()),
    StructField("carrier", StringType())
])

# ============================================================
# HELPER: Write batch to PostgreSQL
# ============================================================
import os as _os
_FF_COMPANY = _os.getenv("FF_COMPANY", "EZZ").strip() or "EZZ"
_FF_FACTORY = _os.getenv("FF_FACTORY", "EZZ_DEMO").strip() or "EZZ_DEMO"

def write_to_postgres(df, table_name):
    """Write a DataFrame to PostgreSQL (tenant-tagged: streaming = demo factory)"""
    try:
        if df.count() > 0:
            # tag streaming rows with the demo tenant if not already present
            if "company_id" not in df.columns:
                df = df.withColumn("company_id", F.lit(_FF_COMPANY))
            if "factory_id" not in df.columns:
                df = df.withColumn("factory_id", F.lit(_FF_FACTORY))
            df.write.mode("append").jdbc(PG_URL, table_name, properties=PG_PROPS)
    except Exception as e:
        print(f"   ERROR writing to {table_name}: {str(e)[:200]}")

# ============================================================
# STREAM 1: PRICE ALERTS
# ============================================================
print("\n" + "=" * 50)
print("STREAM 1: Price Monitoring (steel_market_prices)")
print("=" * 50)

def process_market_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    processed = batch_df \
        .withColumn("steel_price", F.col("steel_price_egypt_egp").cast("double")) \
        .withColumn("iron_ore", F.col("iron_ore_price_usd").cast("double")) \
        .withColumn("usd_rate", F.col("usd_egp_rate").cast("double")) \
        .withColumn("oil_price", F.col("brent_oil_usd").cast("double"))
    
    # Detect price spikes (>5% change simulation - compare to avg ~42000)
    avg_price = 42000.0
    alerts = processed \
        .withColumn("change_pct", F.round((F.col("steel_price") - F.lit(avg_price)) / F.lit(avg_price) * 100, 2)) \
        .withColumn("alert_level",
            F.when(F.abs(F.col("change_pct")) > 15, "CRITICAL")
            .when(F.abs(F.col("change_pct")) > 10, "HIGH")
            .when(F.abs(F.col("change_pct")) > 5, "MEDIUM")
            .otherwise("LOW")
        ) \
        .select(
            F.col("event_timestamp").cast("timestamp").alias("alert_time"),
            F.lit("steel_price_egp").alias("price_type"),
            F.col("steel_price").alias("current_price"),
            F.lit(avg_price).alias("previous_price"),
            "change_pct",
            "alert_level"
        ) \
        .withColumn("created_at", F.current_timestamp())
    
    write_to_postgres(alerts, "streaming.price_alerts")
    
    cnt = alerts.count()
    high_alerts = alerts.filter(F.col("alert_level").isin("HIGH", "CRITICAL")).count()
    print(f"   [Batch {batch_id}] Market: {cnt} events, {high_alerts} high/critical alerts")

market_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "steel_market_prices") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), market_schema).alias("data")) \
    .select("data.*")

market_query = market_stream.writeStream \
    .foreachBatch(process_market_batch) \
    .trigger(processingTime="15 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/market") \
    .queryName("market_alerts") \
    .start()

print("   Market stream started!")

# ============================================================
# STREAM 2: ORDER TRACKING
# ============================================================
print("\n" + "=" * 50)
print("STREAM 2: Order Tracking (steel_orders)")
print("=" * 50)

def process_orders_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    processed = batch_df \
        .withColumn("qty", F.col("quantity_tons").cast("double")) \
        .withColumn("revenue", F.col("total_value_egp").cast("double"))
    
    summary = processed.agg(
        F.count("*").alias("total_orders"),
        F.round(F.sum("qty"), 2).alias("total_quantity_tons"),
        F.round(F.sum("revenue"), 2).alias("total_revenue_egp"),
        F.sum(F.when(F.col("qty") >= 1000, 1).otherwise(0)).alias("large_orders")
    ) \
    .withColumn("window_start", F.current_timestamp()) \
    .withColumn("window_end", F.current_timestamp()) \
    .withColumn("created_at", F.current_timestamp())
    
    # Get top product
    top_prod = processed.groupBy("product_type").agg(F.sum("qty").alias("qty_sum")) \
        .orderBy(F.desc("qty_sum")).limit(1).collect()
    
    top_product = top_prod[0]["product_type"] if top_prod else "unknown"
    
    summary = summary.withColumn("top_product", F.lit(top_product))
    
    write_to_postgres(summary, "streaming.order_summary")
    
    total = processed.count()
    large = processed.filter(F.col("qty") >= 1000).count()
    print(f"   [Batch {batch_id}] Orders: {total} events, {large} large orders (>=1000 tons)")

orders_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "steel_orders") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 20) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), orders_schema).alias("data")) \
    .select("data.*")

orders_query = orders_stream.writeStream \
    .foreachBatch(process_orders_batch) \
    .trigger(processingTime="20 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/orders") \
    .queryName("order_tracking") \
    .start()

print("   Orders stream started!")

# ============================================================
# STREAM 3: PRODUCTION MONITORING
# ============================================================
print("\n" + "=" * 50)
print("STREAM 3: Production Monitoring (steel_production)")
print("=" * 50)

def process_production_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    processed = batch_df \
        .withColumn("eff", F.col("efficiency_pct").cast("double")) \
        .withColumn("tons", F.col("actual_tons").cast("double")) \
        .withColumn("quality", F.col("quality_score").cast("double"))
    
    alerts = processed \
        .withColumn("alert",
            F.when(F.col("status") == "power_outage", "POWER_OUTAGE")
            .when(F.col("status") == "maintenance", "MAINTENANCE")
            .when(F.col("eff") < 70, "LOW_EFFICIENCY")
            .when(F.col("quality") < 7.0, "LOW_QUALITY")
            .otherwise("NORMAL")
        ) \
        .select(
            F.col("event_timestamp").cast("timestamp").alias("event_time"),
            "facility",
            "production_line",
            F.col("tons").alias("actual_tons"),
            F.col("eff").alias("efficiency_pct"),
            F.col("quality").alias("quality_score"),
            "alert"
        ) \
        .withColumn("created_at", F.current_timestamp())
    
    write_to_postgres(alerts, "streaming.production_live")
    
    cnt = alerts.count()
    issues = alerts.filter(F.col("alert") != "NORMAL").count()
    print(f"   [Batch {batch_id}] Production: {cnt} events, {issues} alerts")

production_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "steel_production") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 15) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), production_schema).alias("data")) \
    .select("data.*")

production_query = production_stream.writeStream \
    .foreachBatch(process_production_batch) \
    .trigger(processingTime="20 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/production") \
    .queryName("production_monitor") \
    .start()

print("   Production stream started!")

# ============================================================
# STREAM 4: SHIPMENT TRACKING
# ============================================================
print("\n" + "=" * 50)
print("STREAM 4: Shipment Tracking (steel_shipments)")
print("=" * 50)

def process_shipments_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    processed = batch_df \
        .withColumn("delay", F.col("delay_days").cast("integer")) \
        .withColumn("weight", F.col("weight_tons").cast("double"))
    
    shipment_status = processed \
        .withColumn("is_delayed", F.when(F.col("delay") > 0, True).otherwise(False)) \
        .select(
            F.col("event_timestamp").cast("timestamp").alias("event_time"),
            "shipment_id",
            "origin",
            "destination",
            "status",
            F.col("delay").alias("delay_days"),
            "is_delayed"
        ) \
        .withColumn("created_at", F.current_timestamp())
    
    write_to_postgres(shipment_status, "streaming.shipment_status")
    
    cnt = shipment_status.count()
    delayed = shipment_status.filter(F.col("is_delayed") == True).count()
    print(f"   [Batch {batch_id}] Shipments: {cnt} events, {delayed} delayed")

shipments_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "steel_shipments") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), shipments_schema).alias("data")) \
    .select("data.*")

shipments_query = shipments_stream.writeStream \
    .foreachBatch(process_shipments_batch) \
    .trigger(processingTime="25 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/shipments") \
    .queryName("shipment_tracking") \
    .start()

print("   Shipments stream started!")

# ============================================================
# MONITOR ALL STREAMS
# ============================================================
print("\n" + "=" * 60)
print("ALL 4 STREAMS RUNNING!")
print("=" * 60)
print("Stream 1: market_alerts     (every 15s)")
print("Stream 2: order_tracking    (every 20s)")
print("Stream 3: production_monitor (every 20s)")
print("Stream 4: shipment_tracking (every 25s)")
print("")
print("Writing to PostgreSQL: streaming.* schema")
print("Press Ctrl+C to stop (or wait 5 minutes for auto-stop)")
print("=" * 60)

# Run for 5 minutes then stop (for demo/testing)
try:
    spark.streams.awaitAnyTermination(timeout=300)
except Exception as e:
    print(f"\nStream timeout or interruption: {e}")

# Print final stats
print("\n" + "=" * 60)
print("STREAMING SUMMARY")
print("=" * 60)

for q in spark.streams.active:
    status = q.status
    print(f"   {q.name}: isDataAvailable={status.get('isDataAvailable', 'N/A')}, "
          f"isTriggerActive={status.get('isTriggerActive', 'N/A')}")

# Stop all streams
for q in spark.streams.active:
    q.stop()

print("\nAll streams stopped.")
print(f"Completed at: {datetime.now()}")
print("=" * 60)

spark.stop()
