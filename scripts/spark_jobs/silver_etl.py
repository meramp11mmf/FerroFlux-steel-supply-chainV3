# ============================================================
# SILVER LAYER ETL - Clean + Transform + Join  (REFACTORED)
# ============================================================
# Changes vs. previous version:
#   * All parquet writes go through etl_common.write_partitioned(),
#     which PURGES legacy/conflicting partition dirs before writing.
#     -> fixes "Conflicting partition column names detected".
#   * Standardised multi-level partition hierarchy:
#         orders_clean    -> company_id, factory_id, region
#         shipments_clean -> company_id, factory_id, transport_mode
#         production_clean-> company_id, factory_id, facility
#         rawmat_clean    -> company_id, factory_id
#         market_clean    -> year                (Egypt-wide / shared)
#   * JDBC URL + credentials built dynamically from env vars.
#   * Division-by-zero guards on all summary stats.
#   * Emits price-spike + underperformance anomalies to n8n.
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

# make sure sibling module is importable regardless of CWD
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import etl_common as ec

print("=" * 60)
print("SILVER LAYER ETL - Starting...")
print(f"Time: {datetime.now()}")
print("=" * 60)

# ------ Spark Session ------
spark = (SparkSession.builder
    .appName("Steel_Supply_Chain_ETL")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
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

BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/spark/data/processed/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/spark/data/processed/silver")

# ── dynamic connection + tenant scope (from etl_common) ──────
PG = ec.pg_conf()
PG_URL = ec.jdbc_url(PG)
PG_PROPS = ec.jdbc_props(PG)
FF_COMPANY, FF_FACTORY = ec.tenant_scope()
SCOPE_COMPANY = FF_COMPANY or None
SCOPE_FACTORY = FF_FACTORY or None
print(f"   Tenant scope: company={FF_COMPANY or '<all>'} factory={FF_FACTORY or '<all>'}")
print(f"   JDBC: {PG_URL}")


def read_bronze(name):
    """Defensive bronze read: clear error if a layer hasn't been ingested."""
    path = f"{BRONZE_PATH}/{name}"
    if not os.path.isdir(path):
        raise FileNotFoundError(
            f"Bronze table not found: {path}. Run bronze_etl.py first.")
    return spark.read.parquet(path)


# ============================================================
# 1. MARKET DATA - Silver  (Egypt-wide, shared across tenants)
# ============================================================
print("\n" + "=" * 50)
print("1/5 - Processing Market Data (Silver)...")
print("=" * 50)

df_market = read_bronze("market_prices")
print(f"   Read {df_market.count():,} rows from bronze")

df_market_base = df_market.withColumn("year", F.year("date"))
w_market = Window.partitionBy("company_id", "factory_id", "year").orderBy("date")
w7 = w_market.rowsBetween(-6, 0)
w30 = w_market.rowsBetween(-29, 0)

df_market_silver = df_market_base \
    .withColumn("prev_price", F.lag("steel_price_egypt_egp", 1).over(w_market)) \
    .withColumn("price_change_pct",
        F.when(F.col("prev_price").isNotNull(),
            F.round((F.col("steel_price_egypt_egp") - F.col("prev_price")) / F.col("prev_price") * 100, 4)
        ).otherwise(0.0)) \
    .withColumn("moving_avg_7d", F.round(F.avg("steel_price_egypt_egp").over(w7), 2)) \
    .withColumn("moving_avg_30d", F.round(F.avg("steel_price_egypt_egp").over(w30), 2)) \
    .withColumn("price_volatility_7d", F.round(F.stddev("steel_price_egypt_egp").over(w7), 2)) \
    .withColumn("is_price_spike", F.when(F.abs(F.col("price_change_pct")) > 2.0, 1).otherwise(0)) \
    .withColumn("iron_ore_price_egp", F.round(F.col("iron_ore_price_usd") * F.col("usd_egp_rate"), 2)) \
    .withColumn("scrap_price_egp", F.round(F.col("scrap_price_usd") * F.col("usd_egp_rate"), 2)) \
    .drop("prev_price", "loaded_at") \
    .withColumn("loaded_at", F.current_timestamp())

cnt = df_market_silver.count()
spikes_df = df_market_silver.filter(F.col("is_price_spike") == 1)
spikes = spikes_df.count()
print(f"   Processed: {cnt:,} rows | price spikes: {spikes}")

# market is global -> company=None forces a clean full rebuild
ec.write_partitioned(df_market_silver, f"{SILVER_PATH}/market_clean", ["year"], company=None)
ec.save_pg_tenant(df_market_silver, "processed_data.market_clean", PG)

# --- n8n alert: price spikes (Workflow 1 trigger) ---
if spikes:
    ec.emit_anomalies_from_df(
        spikes_df, "price_spike",
        ["company_id", "factory_id", "date", "steel_price_egypt_egp",
         "price_change_pct", "is_price_spike"])
print("   Saved to silver/market_clean + PostgreSQL")

# ============================================================
# 2. PRODUCTION DATA - Silver
# ============================================================
print("\n" + "=" * 50)
print("2/5 - Processing Production Data (Silver)...")
print("=" * 50)

df_prod = ec.apply_scope(read_bronze("production"))
print(f"   Read {df_prod.count():,} rows from bronze")

df_market_elec = df_market_silver.select(
    F.col("date").alias("m_date"),
    "electricity_price_egp_kwh", "natural_gas_price_usd", "usd_egp_rate")

shift_rank_expr = F.when(F.col("shift") == "morning", 1) \
    .when(F.col("shift") == "afternoon", 2) \
    .when(F.col("shift") == "night", 3).otherwise(4)

df_prod_silver = df_prod \
    .join(df_market_elec, df_prod["date"] == df_market_elec["m_date"], "left") \
    .drop("m_date") \
    .withColumn("net_output_tons", F.round(F.col("actual_tons") - F.col("waste_tons"), 2)) \
    .withColumn("energy_per_ton",
        F.when(F.col("actual_tons") > 0, F.round(F.col("energy_kwh") / F.col("actual_tons"), 2)).otherwise(0)) \
    .withColumn("gas_per_ton",
        F.when(F.col("actual_tons") > 0, F.round(F.col("natural_gas_m3") / F.col("actual_tons"), 2)).otherwise(0)) \
    .withColumn("estimated_energy_cost_egp",
        F.round(
            F.col("energy_kwh") * F.coalesce(F.col("electricity_price_egp_kwh"), F.lit(1.5)) +
            F.col("natural_gas_m3") * F.coalesce(F.col("natural_gas_price_usd"), F.lit(3.0)) * F.coalesce(F.col("usd_egp_rate"), F.lit(30.0)),
            2)) \
    .withColumn("shift_rank", shift_rank_expr) \
    .withColumn("is_underperforming", F.when(F.col("efficiency_pct") < 70, 1).otherwise(0)) \
    .drop("electricity_price_egp_kwh", "natural_gas_price_usd", "usd_egp_rate", "loaded_at") \
    .withColumn("loaded_at", F.current_timestamp())

cnt = df_prod_silver.count()
under_df = df_prod_silver.filter(F.col("is_underperforming") == 1)
underperf = under_df.count()
print(f"   Processed: {cnt:,} rows | underperforming (<70%): {underperf} ({ec.safe_pct(underperf, cnt):.1f}%)")

ec.write_partitioned(
    df_prod_silver, f"{SILVER_PATH}/production_clean",
    ["company_id", "factory_id", "facility"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(df_prod_silver, "processed_data.production_clean", PG)

# --- n8n alert: efficiency < 70% (Workflow 1 trigger) ---
if underperf:
    ec.emit_anomalies_from_df(
        under_df, "low_efficiency",
        ["company_id", "factory_id", "facility", "production_line",
         "date", "efficiency_pct", "is_underperforming"])
print("   Saved to silver/production_clean + PostgreSQL")

# ============================================================
# 3. ORDERS DATA - Silver
# ============================================================
print("\n" + "=" * 50)
print("3/5 - Processing Orders Data (Silver)...")
print("=" * 50)

df_orders = ec.apply_scope(read_bronze("orders"))
print(f"   Read {df_orders.count():,} rows from bronze")

df_market_price = df_market_silver.select(
    F.col("date").alias("m_date"),
    F.col("steel_price_egypt_egp").alias("steel_price_at_order"),
    F.col("usd_egp_rate").alias("usd_rate_at_order"))

region_expr = (
    F.when(F.col("delivery_governorate").isin("Cairo", "Giza", "Qalyubia"), "Greater_Cairo")
    .when(F.col("delivery_governorate").isin("Alexandria", "Beheira", "Matrouh"), "Alexandria_Region")
    .when(F.col("delivery_governorate").isin("Dakahlia", "Sharqia", "Gharbia", "Monufia", "Kafr_El_Sheikh", "Damietta"), "Delta")
    .when(F.col("delivery_governorate").isin("Suez", "Ismailia", "Port_Said"), "Canal_Zone")
    .when(F.col("delivery_governorate").isin("Fayoum", "Beni_Suef", "Minya"), "Middle_Egypt")
    .when(F.col("delivery_governorate").isin("Assiut", "Sohag", "Qena", "Luxor", "Aswan"), "Upper_Egypt")
    .when(F.col("delivery_governorate").isin("Red_Sea", "South_Sinai", "North_Sinai", "New_Valley"), "Frontier")
    .otherwise("Other"))

df_orders_silver = df_orders \
    .join(df_market_price, df_orders["order_date"] == df_market_price["m_date"], "left") \
    .drop("m_date") \
    .withColumn("is_large_order", F.when(F.col("quantity_tons") >= 100, 1).otherwise(0)) \
    .withColumn("region", region_expr) \
    .withColumn("delivery_performance_score",
        F.when(F.col("status") == "cancelled", F.lit(0.0))
        .when(F.col("delay_days") <= 0, F.lit(100.0))
        .when(F.col("delay_days") <= 2, F.lit(80.0))
        .when(F.col("delay_days") <= 5, F.lit(60.0))
        .when(F.col("delay_days") <= 10, F.lit(40.0))
        .otherwise(F.lit(20.0))) \
    .withColumn("order_month", F.month("order_date")) \
    .withColumn("order_quarter", F.quarter("order_date")) \
    .withColumn("order_day_of_week", F.dayofweek("order_date")) \
    .drop("loaded_at") \
    .withColumn("loaded_at", F.current_timestamp())

cnt = df_orders_silver.count()
large = df_orders_silver.filter(F.col("is_large_order") == 1).count()
print(f"   Processed: {cnt:,} rows | large orders (>=100t): {large} ({ec.safe_pct(large, cnt):.1f}%)")

# STANDARDISED: company_id, factory_id, region
ec.write_partitioned(
    df_orders_silver, f"{SILVER_PATH}/orders_clean",
    ["company_id", "factory_id", "region"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(df_orders_silver, "processed_data.orders_clean", PG)
print("   Saved to silver/orders_clean + PostgreSQL")

# ============================================================
# 4. SHIPMENTS DATA - Silver
# ============================================================
print("\n" + "=" * 50)
print("4/5 - Processing Shipments Data (Silver)...")
print("=" * 50)

df_ship = ec.apply_scope(read_bronze("shipments"))
print(f"   Read {df_ship.count():,} rows from bronze")

df_ship_silver = df_ship \
    .withColumn("cost_per_ton",
        F.when(F.col("weight_tons") > 0, F.round(F.col("transport_cost_egp") / F.col("weight_tons"), 2)).otherwise(0)) \
    .withColumn("transit_days", F.datediff(F.col("arrival_date"), F.col("departure_date"))) \
    .withColumn("delivery_speed_km_per_day",
        F.when(F.datediff(F.col("arrival_date"), F.col("departure_date")) > 0,
            F.round(F.col("distance_km") / F.datediff(F.col("arrival_date"), F.col("departure_date")), 2)
        ).otherwise(F.col("distance_km"))) \
    .withColumn("fuel_per_ton",
        F.when(F.col("weight_tons") > 0, F.round(F.col("fuel_liters") / F.col("weight_tons"), 2)).otherwise(0)) \
    .withColumn("co2_per_ton",
        F.when(F.col("weight_tons") > 0, F.round(F.col("co2_emissions_kg") / F.col("weight_tons"), 2)).otherwise(0)) \
    .withColumn("is_efficient", F.when(F.col("cost_per_ton_km_egp") <= 3.5, 1).otherwise(0)) \
    .drop("loaded_at") \
    .withColumn("loaded_at", F.current_timestamp())

cnt = df_ship_silver.count()
efficient = df_ship_silver.filter(F.col("is_efficient") == 1).count()
avg_transit_row = df_ship_silver.agg(F.avg("transit_days")).collect()[0][0]
avg_transit = avg_transit_row if avg_transit_row is not None else 0.0
print(f"   Processed: {cnt:,} rows | efficient: {efficient} ({ec.safe_pct(efficient, cnt):.1f}%) | avg transit: {avg_transit:.1f}d")

# STANDARDISED: company_id, factory_id, transport_mode
ec.write_partitioned(
    df_ship_silver, f"{SILVER_PATH}/shipments_clean",
    ["company_id", "factory_id", "transport_mode"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(df_ship_silver, "processed_data.shipments_clean", PG)
print("   Saved to silver/shipments_clean + PostgreSQL")

# ============================================================
# 5. RAW MATERIALS DATA - Silver
# ============================================================
print("\n" + "=" * 50)
print("5/5 - Processing Raw Materials Data (Silver)...")
print("=" * 50)

df_rawmat = ec.apply_scope(read_bronze("raw_materials"))
print(f"   Read {df_rawmat.count():,} rows from bronze")

df_market_fx = df_market_silver.select(
    F.col("date").alias("m_date"),
    F.col("usd_egp_rate").alias("usd_rate_at_purchase"))

df_rawmat_silver = df_rawmat \
    .join(df_market_fx, df_rawmat["purchase_date"] == df_market_fx["m_date"], "left") \
    .drop("m_date") \
    .withColumn("total_landed_cost_egp",
        F.round(F.col("total_landed_cost_usd") * F.coalesce(F.col("usd_rate_at_purchase"), F.lit(30.0)), 2)) \
    .withColumn("days_late",
        F.when(F.col("actual_delivery").isNotNull() & F.col("expected_delivery").isNotNull(),
            F.datediff(F.col("actual_delivery"), F.col("expected_delivery"))).otherwise(0)) \
    .withColumn("lead_time_days",
        F.when(F.col("actual_delivery").isNotNull() & F.col("purchase_date").isNotNull(),
            F.datediff(F.col("actual_delivery"), F.col("purchase_date"))).otherwise(0)) \
    .withColumn("price_per_ton_egp",
        F.round(F.col("price_per_ton_usd") * F.coalesce(F.col("usd_rate_at_purchase"), F.lit(30.0)), 2)) \
    .drop("loaded_at") \
    .withColumn("loaded_at", F.current_timestamp())

cnt = df_rawmat_silver.count()
on_time = df_rawmat_silver.filter(F.col("on_time") == 1).count()
avg_lead_row = df_rawmat_silver.agg(F.avg("lead_time_days")).collect()[0][0]
avg_lead = avg_lead_row if avg_lead_row is not None else 0.0
print(f"   Processed: {cnt:,} rows | on-time: {on_time} ({ec.safe_pct(on_time, cnt):.1f}%) | avg lead: {avg_lead:.0f}d")

# STANDARDISED: company_id, factory_id
ec.write_partitioned(
    df_rawmat_silver, f"{SILVER_PATH}/rawmat_clean",
    ["company_id", "factory_id"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(df_rawmat_silver, "processed_data.rawmat_clean", PG)
print("   Saved to silver/rawmat_clean + PostgreSQL")

# ============================================================
print("\n" + "=" * 60)
print("SILVER LAYER ETL - COMPLETE!")
print(f"Completed at: {datetime.now()}")
print("=" * 60)
spark.stop()
