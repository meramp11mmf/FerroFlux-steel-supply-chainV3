# ============================================================
# GOLD LAYER ETL - Aggregated KPIs from Silver  (REFACTORED)
# ============================================================
# Changes vs. previous version:
#   * All parquet writes go through etl_common.write_partitioned()
#     (purge legacy layout -> uniform partitioning -> no conflict crash).
#   * BUGFIX: supplier_scorecard previously dropped company_id/factory_id
#     in its .select() and then tried to .partitionBy("company_id",...)
#     -> AnalysisException. Tenant columns are now kept.
#   * Standardised partition hierarchy on every tenant table:
#         daily_kpis            -> company_id, factory_id, year
#         monthly_summary       -> company_id, factory_id, year_val, month_val
#         supplier_scorecard    -> company_id, factory_id, material_type
#         regional_demand       -> company_id, factory_id, region
#         production_efficiency -> company_id, factory_id, facility
#         price_features        -> year                 (Egypt-wide / shared)
#   * Dynamic JDBC from env, zero-division guards, n8n alerts.
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import etl_common as ec

print("=" * 60)
print("GOLD LAYER ETL - Starting...")
print(f"Time: {datetime.now()}")
print("=" * 60)

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
    .config("spark.hadoop.parquet.summary.metadata.level", "NONE")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
# Silence WindowExec partition warning — intentionally unpartitioned for price_features
# (see comment above w_date definition for rationale)
spark.sparkContext._jvm.org.apache.log4j.LogManager \
    .getLogger("org.apache.spark.sql.execution.window.WindowExec") \
    .setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.ERROR)

SILVER_PATH = os.getenv("SILVER_PATH", "/opt/spark/data/processed/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/opt/spark/data/processed/gold")

PG = ec.pg_conf()
PG_URL = ec.jdbc_url(PG)
FF_COMPANY, FF_FACTORY = ec.tenant_scope()
SCOPE_COMPANY = FF_COMPANY or None
SCOPE_FACTORY = FF_FACTORY or None
print(f"   Tenant scope: company={FF_COMPANY or '<all>'} factory={FF_FACTORY or '<all>'}")
print(f"   JDBC: {PG_URL}")


def read_silver(name, scoped=True):
    path = f"{SILVER_PATH}/{name}"
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Silver table not found: {path}. Run silver_etl.py first.")
    df = spark.read.parquet(path)
    return ec.apply_scope(df) if scoped else df


# ============================================================
# LOAD SILVER
# ============================================================
print("\n" + "=" * 50)
print("Loading Silver-layer tables...")
print("=" * 50)

df_mkt    = read_silver("market_clean", scoped=False)  # Egypt-wide / shared
df_prod   = read_silver("production_clean")
df_orders = read_silver("orders_clean")
df_ship   = read_silver("shipments_clean")
df_rawmat = read_silver("rawmat_clean")

print(f"   market_clean:     {df_mkt.count():>8,} rows")
print(f"   production_clean: {df_prod.count():>8,} rows")
print(f"   orders_clean:     {df_orders.count():>8,} rows")
print(f"   shipments_clean:  {df_ship.count():>8,} rows")
print(f"   rawmat_clean:     {df_rawmat.count():>8,} rows")

# ============================================================
# TABLE 1 — DAILY KPIs
# ============================================================
print("\n" + "=" * 50)
print("TABLE 1 / 6 — daily_kpis")
print("=" * 50)

daily_prod = df_prod.groupBy("company_id", "factory_id", "date").agg(
    F.sum("actual_tons").alias("total_production_tons"),
    F.avg("efficiency_pct").alias("avg_efficiency"),
    F.sum("waste_tons").alias("total_waste_tons"),
    F.sum("energy_kwh").alias("total_energy_kwh"),
    F.sum(F.when(F.col("efficiency_pct") < 70, 1).otherwise(0)).alias("underperforming_batches"))

daily_orders = df_orders.groupBy("company_id", "factory_id", "order_date").agg(
    F.count("order_id").alias("total_orders"),
    F.sum("quantity_tons").alias("total_order_tons"),
    F.sum("total_value_egp").alias("total_revenue_egp"),
    F.avg("quantity_tons").alias("avg_order_size_tons"),
    F.round(F.sum(F.when(F.col("status") == "delivered", 1).otherwise(0)) /
            F.count("order_id") * 100, 2).alias("on_time_delivery_pct"),
    F.sum("quantity_tons").alias("total_demand_tons"),
    F.sum(F.when(F.col("status").isin("cancelled", "Cancelled"), 1).otherwise(0)).alias("cancelled_orders_daily"))

daily_ship = df_ship.groupBy("company_id", "factory_id", "departure_date").agg(
    F.count("shipment_id").alias("total_shipments"),
    F.sum("transport_cost_egp").alias("total_logistics_cost_egp"),
    F.sum("co2_emissions_kg").alias("total_co2_kg"),
    F.sum("fuel_liters").alias("total_fuel_liters"),
    F.avg("cost_per_ton_km_egp").alias("avg_cost_per_ton_km"))

daily_kpis_df = daily_prod.join(
        daily_orders,
        (daily_prod["company_id"] == daily_orders["company_id"]) &
        (daily_prod["factory_id"] == daily_orders["factory_id"]) &
        (daily_prod["date"] == daily_orders["order_date"]), "left") \
    .join(daily_ship,
        (daily_prod["company_id"] == daily_ship["company_id"]) &
        (daily_prod["factory_id"] == daily_ship["factory_id"]) &
        (daily_prod["date"] == daily_ship["departure_date"]), "left") \
    .join(df_mkt, daily_prod["date"] == df_mkt["date"], "left") \
    .select(
        daily_prod["company_id"], daily_prod["factory_id"], daily_prod["date"],
        "total_production_tons", "avg_efficiency", "total_waste_tons",
        "total_orders", "cancelled_orders_daily", "total_revenue_egp",
        "avg_order_size_tons", "on_time_delivery_pct",
        F.col("steel_price_egypt_egp").alias("steel_price_egp"),
        F.col("usd_egp_rate"), "total_shipments",
        "total_logistics_cost_egp", "total_co2_kg") \
    .withColumn("year", F.year("date"))

daily_kpis_df = daily_kpis_df.withColumn(
    "profit_estimate_egp",
    F.round(
        F.coalesce("total_revenue_egp", F.lit(0)) -
        F.coalesce("total_production_tons", F.lit(0)) * F.coalesce("steel_price_egp", F.lit(1000)) * 0.6 -
        F.coalesce("total_logistics_cost_egp", F.lit(0)) -
        F.greatest("total_waste_tons", F.lit(0)) * 200, 2)) \
    .withColumn("loaded_at", F.current_timestamp())

cnt_daily = daily_kpis_df.count()
print(f"   Built: {cnt_daily:,} daily rows")

ec.write_partitioned(
    daily_kpis_df, f"{GOLD_PATH}/daily_kpis",
    ["company_id", "factory_id", "year"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(daily_kpis_df, "analytics.daily_kpis", PG)
print("   Saved to gold/daily_kpis + PostgreSQL")

# ============================================================
# TABLE 2 — MONTHLY SUMMARY
# ============================================================
print("\n" + "=" * 50)
print("TABLE 2 / 6 — monthly_summary")
print("=" * 50)

monthly_df = daily_kpis_df \
    .withColumn("year_val", F.year("date")) \
    .withColumn("month_val", F.month("date")) \
    .withColumn("month_start", F.make_date("year_val", "month_val", F.lit(1))) \
    .groupBy("company_id", "factory_id", "month_start", "year_val", "month_val").agg(
        F.sum("total_production_tons").alias("total_production_tons"),
        F.sum("total_revenue_egp").alias("total_revenue_egp"),
        F.sum("total_orders").alias("total_orders"),
        F.sum("cancelled_orders_daily").alias("cancelled_orders"),
        F.sum("total_logistics_cost_egp").alias("total_logistics_cost_egp"),
        F.avg("avg_efficiency").alias("avg_efficiency"),
        F.sum("total_co2_kg").alias("total_co2_kg"),
        F.avg("steel_price_egp").alias("avg_steel_price_egp"),
        F.avg("usd_egp_rate").alias("avg_usd_rate"))

monthly_df = monthly_df.withColumn(
    "gross_margin_pct",
    F.round(
        F.when(F.col("total_revenue_egp") > 0,
            (F.col("total_revenue_egp") - F.col("total_production_tons") * F.col("avg_steel_price_egp") * 0.6
             - F.col("total_logistics_cost_egp")) / F.col("total_revenue_egp") * 100
        ).otherwise(F.lit(0.0)), 2))

monthly_order_flag = df_orders \
    .withColumn("year_val", F.year("order_date")) \
    .withColumn("month_val", F.month("order_date")) \
    .groupBy("company_id", "factory_id", "year_val", "month_val", "product_type") \
    .agg(F.count("*").alias("cnt")) \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("company_id", "factory_id", "year_val", "month_val").orderBy(F.desc("cnt")))) \
    .filter(F.col("rank") == 1) \
    .select("company_id", "factory_id", "year_val", "month_val", F.col("product_type").alias("top_product"))

monthly_gov_flag = df_orders \
    .withColumn("year_val", F.year("order_date")) \
    .withColumn("month_val", F.month("order_date")) \
    .groupBy("company_id", "factory_id", "year_val", "month_val", "delivery_governorate") \
    .agg(F.sum("total_value_egp").alias("gov_rev")) \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("company_id", "factory_id", "year_val", "month_val").orderBy(F.desc("gov_rev")))) \
    .filter(F.col("rank") == 1) \
    .select("company_id", "factory_id", "year_val", "month_val", F.col("delivery_governorate").alias("top_governorate"))

monthly_summary = monthly_df \
    .join(monthly_order_flag, ["company_id", "factory_id", "year_val", "month_val"], "left") \
    .join(monthly_gov_flag, ["company_id", "factory_id", "year_val", "month_val"], "left") \
    .select(
        "company_id", "factory_id", "month_start", "year_val", "month_val",
        "total_production_tons", "total_revenue_egp", "total_orders", "cancelled_orders",
        "gross_margin_pct", "avg_efficiency", "total_co2_kg", "avg_steel_price_egp",
        "avg_usd_rate", "top_product", "top_governorate") \
    .withColumn("loaded_at", F.current_timestamp())

cnt_monthly = monthly_summary.count()
print(f"   Built: {cnt_monthly} months")

ec.write_partitioned(
    monthly_summary, f"{GOLD_PATH}/monthly_summary",
    ["company_id", "factory_id", "year_val", "month_val"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(monthly_summary, "analytics.monthly_summary", PG)
print("   Saved to gold/monthly_summary + PostgreSQL")

# ============================================================
# TABLE 3 — SUPPLIER SCORECARD   (BUGFIX: keep tenant columns)
# ============================================================
print("\n" + "=" * 50)
print("TABLE 3 / 6 — supplier_scorecard")
print("=" * 50)

supplier_scorecard = df_rawmat \
    .join(df_mkt.select("date", "usd_egp_rate", "steel_price_egypt_egp"),
          df_rawmat["purchase_date"] == df_mkt["date"], "left") \
    .withColumn("price_per_ton_egp",
        F.round(F.col("price_per_ton_usd") * F.coalesce(F.col("usd_egp_rate"), F.lit(30.0)), 2)) \
    .groupBy("company_id", "factory_id", "supplier_name", "origin_country", "material_type").agg(
        F.count("purchase_id").alias("total_purchases"),
        F.sum("quantity_tons").alias("total_quantity_tons"),
        F.avg("price_per_ton_usd").alias("avg_price_per_ton_usd"),
        F.avg("lead_time_days").alias("avg_lead_time_days"),
        F.round(F.sum(F.when(F.col("on_time") == 1, 1).otherwise(0)) / F.count("purchase_id") * 100, 2).alias("on_time_pct"),
        F.avg("supplier_reliability").alias("avg_reliability"),
        F.stddev("price_per_ton_usd").alias("price_volatility"))

supplier_scorecard = supplier_scorecard \
    .withColumn("on_time_score",
        F.round(F.when(F.col("on_time_pct") >= 80, 10).when(F.col("on_time_pct") >= 50, 20).otherwise(40), 2)) \
    .withColumn("quality_score", F.round(F.greatest("avg_reliability", F.lit(0)) * 3, 2)) \
    .withColumn("price_score",
        F.round(
            F.when(F.col("price_volatility").isNull(), 15)
            .when(F.col("price_volatility") <= 5, 10)
            .when(F.col("price_volatility") <= 15, 20)
            .when(F.col("price_volatility") <= 30, 30).otherwise(45), 2)) \
    .withColumn("risk_score", F.round(F.col("on_time_score") + F.col("quality_score") + F.col("price_score"), 2)) \
    .select(
        "company_id", "factory_id",          # <-- KEPT (previously dropped -> crash)
        "supplier_name", "origin_country", "material_type",
        "total_purchases", "total_quantity_tons", "avg_price_per_ton_usd",
        "avg_lead_time_days", "on_time_pct", "avg_reliability",
        F.col("avg_reliability").alias("avg_quality_grade"),
        "price_volatility", "risk_score") \
    .withColumn("loaded_at", F.current_timestamp())

cnt_supplier = supplier_scorecard.count()
print(f"   Built: {cnt_supplier} suppliers")

ec.write_partitioned(
    supplier_scorecard, f"{GOLD_PATH}/supplier_scorecard",
    ["company_id", "factory_id", "material_type"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(supplier_scorecard, "analytics.supplier_scorecard", PG)
print("   Saved to gold/supplier_scorecard + PostgreSQL")

# ============================================================
# TABLE 4 — REGIONAL DEMAND
# ============================================================
print("\n" + "=" * 50)
print("TABLE 4 / 6 — regional_demand")
print("=" * 50)

orders_reg = df_orders.select(
    "company_id", "factory_id", "order_id", "order_date", "product_type",
    "customer_type", "quantity_tons", "total_value_egp", "delay_days",
    "delivery_governorate", "region")

ship_reg = df_ship.select(
    F.col("company_id").alias("s_company_id"),
    F.col("factory_id").alias("s_factory_id"),
    F.col("order_id").alias("s_order_id"), "transport_cost_egp")

regional_demand = orders_reg \
    .join(ship_reg,
          (orders_reg["company_id"] == ship_reg["s_company_id"]) &
          (orders_reg["factory_id"] == ship_reg["s_factory_id"]) &
          (orders_reg["order_id"] == ship_reg["s_order_id"]), "left") \
    .groupBy("company_id", "factory_id", "delivery_governorate", "region").agg(
        F.count_distinct("order_id").alias("total_orders"),
        F.sum("quantity_tons").alias("total_quantity_tons"),
        F.sum("total_value_egp").alias("total_revenue_egp"),
        F.avg("quantity_tons").alias("avg_order_size_tons"),
        F.avg("delay_days").alias("avg_delivery_days"),
        F.round(F.sum(F.when(F.col("delay_days") > 0, 1).otherwise(0)) / F.count_distinct("order_id") * 100, 2).alias("delay_pct"),
        F.first("product_type").alias("top_product"),
        F.first("customer_type").alias("top_customer_type"),
        F.avg("transport_cost_egp").alias("avg_transport_cost_egp")) \
    .withColumn("loaded_at", F.current_timestamp()) \
    .withColumnRenamed("delivery_governorate", "governorate")

print(f"   Built: {regional_demand.count()} governorate regions")

ec.write_partitioned(
    regional_demand, f"{GOLD_PATH}/regional_demand",
    ["company_id", "factory_id", "region"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(regional_demand, "analytics.regional_demand", PG)
print("   Saved to gold/regional_demand + PostgreSQL")

# ============================================================
# TABLE 5 — PRODUCTION EFFICIENCY
# ============================================================
print("\n" + "=" * 50)
print("TABLE 5 / 6 — production_efficiency")
print("=" * 50)

shift_rank_expr = F.when(F.col("shift") == "morning", 1) \
    .when(F.col("shift") == "afternoon", 2).when(F.col("shift") == "night", 3).otherwise(4)

prod_efficiency = df_prod \
    .withColumn("net_output_tons", F.col("actual_tons") - F.col("waste_tons")) \
    .withColumn("shift_rank", shift_rank_expr)

base_prod = prod_efficiency \
    .groupBy("company_id", "factory_id", "production_line", "facility", "line_type").agg(
        F.sum("actual_tons").alias("total_output_tons"),
        F.avg("efficiency_pct").alias("avg_efficiency"),
        F.avg("yield_loss_pct").alias("avg_waste_pct"),
        F.avg("quality_score").alias("avg_quality_score"),
        F.avg(F.when(F.col("actual_tons") > 0, F.col("energy_kwh") / F.col("actual_tons")).otherwise(None)).alias("energy_per_ton_kwh"),
        F.avg(F.when(F.col("actual_tons") > 0, F.col("natural_gas_m3") / F.col("actual_tons")).otherwise(None)).alias("gas_per_ton_m3"),
        F.count("batch_id").alias("total_batches"),
        F.round(F.sum(F.when(F.col("status").isin("maintenance", "power_outage"), 1).otherwise(0)) / F.count("batch_id") * 100, 2).alias("downtime_pct"))

shift_by_eff = prod_efficiency \
    .groupBy("company_id", "factory_id", "production_line", "shift") \
    .agg(F.avg("efficiency_pct").alias("avg_eff_shift"))

best_shift_s = shift_by_eff \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("company_id", "factory_id", "production_line").orderBy(F.desc("avg_eff_shift")))) \
    .filter(F.col("rank") == 1) \
    .select("company_id", "factory_id", "production_line", F.col("shift").alias("best_shift"))

wrst_shift_s = shift_by_eff \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("company_id", "factory_id", "production_line").orderBy(F.asc("avg_eff_shift")))) \
    .filter(F.col("rank") == 1) \
    .select("company_id", "factory_id", "production_line", F.col("shift").alias("worst_shift"))

prod_eff_final = base_prod \
    .join(best_shift_s, ["company_id", "factory_id", "production_line"], "left") \
    .join(wrst_shift_s, ["company_id", "factory_id", "production_line"], "left") \
    .select(
        "company_id", "factory_id", "production_line", "facility", "line_type",
        "total_output_tons", "avg_efficiency", "avg_waste_pct", "avg_quality_score",
        "energy_per_ton_kwh", "gas_per_ton_m3", "best_shift", "worst_shift",
        "total_batches", "downtime_pct") \
    .withColumn("loaded_at", F.current_timestamp())

cnt_prod_eff = prod_eff_final.count()
print(f"   Built: {cnt_prod_eff} production lines")

ec.write_partitioned(
    prod_eff_final, f"{GOLD_PATH}/production_efficiency",
    ["company_id", "factory_id", "facility"], company=SCOPE_COMPANY, factory=SCOPE_FACTORY)
ec.save_pg_tenant(prod_eff_final, "analytics.production_efficiency", PG)

# --- n8n alert: production lines averaging < 70% efficiency ---
low_lines = prod_eff_final.filter(F.col("avg_efficiency") < 70)
if low_lines.count():
    ec.emit_anomalies_from_df(
        low_lines, "line_efficiency_low",
        ["company_id", "factory_id", "facility", "production_line", "avg_efficiency"])
print("   Saved to gold/production_efficiency + PostgreSQL")

# ============================================================
# TABLE 6 — PRICE FEATURES (ML, Egypt-wide / shared)
# ============================================================
print("\n" + "=" * 50)
print("TABLE 6 / 6 — price_features (ML input)")
print("=" * 50)

# Global date window — no partitionBy is intentional.
# Partitioning by year would reset lag_1d/lag_7d/etc. at Jan 1 each year,
# making the first N rows of every year NULL even when prior-year data exists.
# Spark will emit a "No Partition Defined for Window" warning — this is expected
# and acceptable for a 730-row global market table on a single-node cluster.
w_date = Window.orderBy("date")

price_features = df_mkt \
    .withColumn("price_lag_1d",  F.lag("steel_price_egypt_egp", 1).over(w_date)) \
    .withColumn("price_lag_7d",  F.lag("steel_price_egypt_egp", 7).over(w_date)) \
    .withColumn("price_lag_14d", F.lag("steel_price_egypt_egp", 14).over(w_date)) \
    .withColumn("price_lag_30d", F.lag("steel_price_egypt_egp", 30).over(w_date)) \
    .withColumn("moving_avg_7d",  F.avg("steel_price_egypt_egp").over(w_date.rowsBetween(-6, 0))) \
    .withColumn("moving_avg_14d", F.avg("steel_price_egypt_egp").over(w_date.rowsBetween(-13, 0))) \
    .withColumn("moving_avg_30d", F.avg("steel_price_egypt_egp").over(w_date.rowsBetween(-29, 0))) \
    .withColumn("price_volatility_7d", F.round(F.stddev("steel_price_egypt_egp").over(w_date.rowsBetween(-6, 0)), 2)) \
    .withColumn("price_volatility_30d", F.round(F.stddev("steel_price_egypt_egp").over(w_date.rowsBetween(-29, 0)), 2)) \
    .withColumn("iron_ore_change_pct",
        F.round((F.col("iron_ore_price_usd") - F.lag("iron_ore_price_usd", 1).over(w_date)) / F.lag("iron_ore_price_usd", 1).over(w_date) * 100, 4)) \
    .withColumn("usd_change_pct",
        F.round((F.col("usd_egp_rate") - F.lag("usd_egp_rate", 1).over(w_date)) / F.lag("usd_egp_rate", 1).over(w_date) * 100, 4)) \
    .withColumn("oil_change_pct",
        F.round((F.col("brent_oil_usd") - F.lag("brent_oil_usd", 1).over(w_date)) / F.lag("brent_oil_usd", 1).over(w_date) * 100, 4)) \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("month_val", F.month("date")) \
    .withColumn("quarter_val", F.quarter("date")) \
    .withColumn("year", F.year("date")) \
    .select(
        "date",
        F.col("steel_price_egypt_egp").alias("steel_price_egp"),
        "iron_ore_price_usd", "scrap_price_usd", "usd_egp_rate", "natural_gas_price_usd",
        "brent_oil_usd", "electricity_price_egp_kwh", "seasonality_index", "is_ramadan",
        "price_change_pct", "price_lag_1d", "price_lag_7d", "price_lag_14d", "price_lag_30d",
        "moving_avg_7d", "moving_avg_14d", "moving_avg_30d", "price_volatility_7d",
        "price_volatility_30d", "iron_ore_change_pct", "usd_change_pct", "oil_change_pct",
        "day_of_week", "month_val", "quarter_val", "year") \
    .withColumn("loaded_at", F.current_timestamp())

cnt_features = price_features.count()
null_lag = price_features.filter(F.col("price_lag_7d").isNull()).count()
print(f"   Built: {cnt_features:,} rows ({ec.safe_pct(null_lag, cnt_features):.1f}% missing leading lags)")

# global table -> partition by year, full rebuild
ec.write_partitioned(price_features, f"{GOLD_PATH}/price_features", ["year"], company=None)

# price_features has no tenant columns — TRUNCATE first to avoid duplicate-key on re-run
try:
    import psycopg2 as _pg2
    _c = _pg2.connect(host=PG["host"], port=PG["port"], dbname=PG["db"],
                      user=PG["user"], password=PG["password"])
    _c.cursor().execute("TRUNCATE TABLE analytics.price_features")
    _c.commit(); _c.close()
    print("   Truncated analytics.price_features before reload")
except Exception as _te:
    print(f"   [warn] TRUNCATE skipped: {_te}")

ec.save_pg_tenant(price_features, "analytics.price_features", PG)
print("   Saved to gold/price_features + PostgreSQL")

# ============================================================
print("\n" + "=" * 60)
print("GOLD LAYER ETL - COMPLETE!")
total_gold_rows = cnt_daily + cnt_monthly + cnt_supplier + cnt_prod_eff + cnt_features
print(f"Total gold rows (approx): {total_gold_rows:,}")
print(f"Completed at: {datetime.now()}")
print("=" * 60)
spark.stop()
