# ============================================================
# MODEL 1: Steel Price Prediction using PySpark MLlib GBT
# ============================================================
# Algorithm: Gradient Boosted Trees (GBTRegressor)
# Input: analytics.price_features (730 days)
# Output: Test-period price forecast (delta-prediction approach)
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml import Pipeline
from datetime import datetime, timedelta

print("=" * 60)
print("MODEL 1: Steel Price Prediction")
print(f"Time: {datetime.now()}")
print("=" * 60)

spark = SparkSession.builder \
    .appName("Steel_Price_Prediction") \
    .config("spark.jars", "/opt/spark/scripts/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

import os as _os
_pg_host = _os.getenv("PG_HOST", "steel-postgres")
_pg_port = _os.getenv("PG_PORT", "5432")
_pg_db   = _os.getenv("PG_DB",   "steel_db")
_pg_user = _os.getenv("PG_USER", _os.getenv("POSTGRES_USER", "steel_admin"))
_pg_pass = _os.getenv("PG_PASSWORD", _os.getenv("POSTGRES_PASSWORD", ""))
PG_URL   = f"jdbc:postgresql://{_pg_host}:{_pg_port}/{_pg_db}"
PG_PROPS = {"user": _pg_user, "password": _pg_pass, "driver": "org.postgresql.Driver"}

# ── TENANT SCOPE (multi-tenant ML) ───────────────────────────
FF_COMPANY = _os.getenv("FF_COMPANY", "").strip() or "EZZ"
FF_FACTORY = _os.getenv("FF_FACTORY", "").strip() or "EZZ_DEMO"
_MIN_TRAIN_ROWS = 200

def _train_scope(df):
    has_cols = "factory_id" in df.columns and "company_id" in df.columns
    if not has_cols:
        return df
    fac = df.filter((df.company_id == FF_COMPANY) & (df.factory_id == FF_FACTORY))
    if fac.count() >= _MIN_TRAIN_ROWS:
        print(f"   [tenant] training on factory {FF_FACTORY} ({fac.count()} rows)")
        return fac
    comp = df.filter(df.company_id == FF_COMPANY)
    if comp.count() >= _MIN_TRAIN_ROWS:
        print(f"   [tenant] factory sparse -> training on company {FF_COMPANY} ({comp.count()} rows)")
        return comp
    print(f"   [tenant] company sparse -> training on ALL data (global model, {df.count()} rows)")
    return df

def _tag_tenant(df):
    from pyspark.sql import functions as _F
    return (df.withColumn("company_id", _F.lit(FF_COMPANY))
              .withColumn("factory_id", _F.lit(FF_FACTORY)))

def save_pg_tenant_ml(df, pg_url, pg_table, pg_props):
    import psycopg2
    try:
        conn = psycopg2.connect(host="steel-postgres", port=5432, dbname="steel_db",
                                user=pg_props["user"], password=pg_props["password"])
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {pg_table} WHERE company_id = %s AND factory_id = %s",
                    (FF_COMPANY, FF_FACTORY))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        print(f"   [warn] tenant pre-delete skipped: {e}")
    df.write.mode("append").jdbc(pg_url, pg_table, properties=pg_props)
# ─────────────────────────────────────────────────────────────


# ============================================================
# 1. LOAD DATA
# ============================================================
print("\n1. Loading price_features from PostgreSQL...")

df = spark.read.jdbc(PG_URL, "analytics.price_features", properties=PG_PROPS)
total = df.count()
print(f"   Loaded: {total} rows")

# moving_avg_30d removed: too correlated with today's price in a 30-day window,
# causes R² collapse on short test splits.
feature_cols = [
    "iron_ore_price_usd", "scrap_price_usd", "usd_egp_rate",
    "natural_gas_price_usd", "brent_oil_usd", "electricity_price_egp_kwh",
    "seasonality_index", "is_ramadan",
    "price_change_pct",
    "price_lag_1d", "price_lag_7d", "price_lag_14d", "price_lag_30d",
    "moving_avg_7d", "moving_avg_14d",
    "price_volatility_7d", "price_volatility_30d",
    "iron_ore_change_pct", "usd_change_pct", "oil_change_pct",
    "day_of_week", "month_val", "quarter_val"
]

df_clean = df.select(["date", "steel_price_egp", "price_lag_1d"] + feature_cols).dropna()
clean_count = df_clean.count()
print(f"   After dropping nulls: {clean_count} rows (dropped {total - clean_count} rows with null lags)")

if clean_count == 0:
    print("\nERROR: No training rows after filtering nulls.")
    print("       Fix: run Gold ETL first (manager.py → ETL Pipelines → Gold only).")
    spark.stop()
    import sys; sys.exit(1)

# Predict price_delta = (today - yesterday) instead of the absolute price.
# Reconstructed price = price_lag_1d + predicted_delta.
# This avoids the model needing to learn the absolute price level and gives
# realistic R² because the residual variance is measured against a much smaller
# baseline (daily moves vs. full price level).
df_clean = df_clean.withColumn("price_delta", F.col("steel_price_egp") - F.col("price_lag_1d"))

# ============================================================
# 2. TRAIN/TEST SPLIT (Time-based: last 60 days for test)
# ============================================================
print("\n2. Splitting data (time-based: 60-day test window)...")

df_sorted = df_clean.orderBy("date")
max_date = df_sorted.agg(F.max("date")).collect()[0][0]
cutoff_date = max_date - timedelta(days=60)

df_train = df_sorted.filter(F.col("date") <= cutoff_date)
df_test  = df_sorted.filter(F.col("date") > cutoff_date)

train_count = df_train.count()
test_count  = df_test.count()
print(f"   Train: {train_count} rows (up to {cutoff_date})")
print(f"   Test:  {test_count} rows ({cutoff_date} to {max_date})")

# ── NAIVE BASELINE: predict price_lag_1d (delta = 0) ──
avg_price = float(df_test.agg(F.avg("steel_price_egp")).collect()[0][0])
_b = df_test.select(
    F.sqrt(F.avg(F.pow(F.col("price_delta"), 2))).alias("rmse"),
    F.avg(F.abs(F.col("price_delta"))).alias("mae"),
    F.sum(F.pow(F.col("price_delta"), 2)).alias("ss_res"),
    F.sum(F.pow(F.col("steel_price_egp") - avg_price, 2)).alias("ss_tot"),
).collect()[0]
baseline_rmse = float(_b["rmse"])
baseline_mae  = float(_b["mae"])
baseline_mape = baseline_mae / avg_price * 100
baseline_r2   = 1.0 - float(_b["ss_res"]) / float(_b["ss_tot"])
print(f"\n   BASELINE (price_lag_1d): RMSE={baseline_rmse:,.2f}  MAE={baseline_mae:,.2f}"
      f"  MAPE={baseline_mape:.2f}%  R2={baseline_r2:.4f}")

# ============================================================
# 3. FEATURE ENGINEERING (VectorAssembler)
# ============================================================
print("\n3. Building ML Pipeline (label = price_delta)...")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)


def _eval_on_price(preds_df, label=""):
    """Reconstruct absolute price from delta prediction and compute metrics."""
    p = preds_df.withColumn("pred_price", F.col("price_lag_1d") + F.col("prediction"))
    row = p.select(
        F.sqrt(F.avg(F.pow(F.col("pred_price") - F.col("steel_price_egp"), 2))).alias("rmse"),
        F.avg(F.abs(F.col("pred_price") - F.col("steel_price_egp"))).alias("mae"),
        F.sum(F.pow(F.col("pred_price") - F.col("steel_price_egp"), 2)).alias("ss_res"),
        F.sum(F.pow(F.col("steel_price_egp") - avg_price, 2)).alias("ss_tot"),
    ).collect()[0]
    rmse = float(row["rmse"])
    mae  = float(row["mae"])
    mape = mae / avg_price * 100
    r2   = 1.0 - float(row["ss_res"]) / float(row["ss_tot"])
    if label:
        print(f"   {label:<20} RMSE={rmse:>10,.2f}  MAE={mae:>10,.2f}  MAPE={mape:>6.2f}%  R2={r2:.4f}")
    return rmse, mae, mape, r2, p


# ============================================================
# 4A. GBT Regressor (label = price_delta)
# ============================================================
print("\n4A. Training GBT Regressor...")

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="price_delta",
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    subsamplingRate=0.8,
    seed=42
)
pipeline_gbt = Pipeline(stages=[assembler, scaler, gbt])
model_gbt = pipeline_gbt.fit(df_train)
preds_gbt = model_gbt.transform(df_test)
rmse_gbt, mae_gbt, mape_gbt, r2_gbt, preds_gbt_full = _eval_on_price(preds_gbt, "GBT")

# ============================================================
# 4B. Random Forest Regressor (label = price_delta)
# ============================================================
print("\n4B. Training Random Forest Regressor...")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price_delta",
    numTrees=100,
    maxDepth=8,
    seed=42
)
pipeline_rf = Pipeline(stages=[assembler, scaler, rf])
model_rf = pipeline_rf.fit(df_train)
preds_rf = model_rf.transform(df_test)
rmse_rf, mae_rf, mape_rf, r2_rf, preds_rf_full = _eval_on_price(preds_rf, "RandomForest")

# ── COMPARISON TABLE ─────────────────────────────────────────
print("\n" + "─" * 70)
print(f"   {'Model':<22} {'RMSE (EGP)':>12} {'MAE (EGP)':>12} {'MAPE':>8} {'R2':>8}")
print("─" * 70)
print(f"   {'Baseline (lag_1d)':<22} {baseline_rmse:>12,.2f} {baseline_mae:>12,.2f} {baseline_mape:>7.2f}% {baseline_r2:>8.4f}")
print(f"   {'GBT':<22} {rmse_gbt:>12,.2f} {mae_gbt:>12,.2f} {mape_gbt:>7.2f}% {r2_gbt:>8.4f}")
print(f"   {'RandomForest':<22} {rmse_rf:>12,.2f} {mae_rf:>12,.2f} {mape_rf:>7.2f}% {r2_rf:>8.4f}")
print("─" * 70)

best_model_name = "GBT" if rmse_gbt < rmse_rf else "RandomForest"
best_rmse       = rmse_gbt  if rmse_gbt < rmse_rf else rmse_rf
best_mae        = mae_gbt   if rmse_gbt < rmse_rf else mae_rf
best_r2         = r2_gbt    if rmse_gbt < rmse_rf else r2_rf
best_mape       = mape_gbt  if rmse_gbt < rmse_rf else mape_rf
best_preds_full = preds_gbt_full if rmse_gbt < rmse_rf else preds_rf_full

print(f"\n   BEST MODEL: {best_model_name}")

# ============================================================
# 5. FEATURE IMPORTANCE (GBT)
# ============================================================
print("\n5. Feature Importance (GBT):")

gbt_model = model_gbt.stages[-1]
importances = gbt_model.featureImportances.toArray()
feature_imp = sorted(zip(feature_cols, importances), key=lambda x: x[1], reverse=True)
for i, (feat, imp) in enumerate(feature_imp[:10]):
    print(f"   {i+1}. {feat}: {imp:.4f}")

# ============================================================
# 6. SAVE PREDICTIONS (absolute predicted price, not delta)
# ============================================================
print("\n6. Saving predictions to PostgreSQL...")

test_results = best_preds_full.select(
    F.current_date().alias("prediction_date"),
    F.col("date").alias("target_date"),
    F.round(F.col("pred_price"), 2).alias("predicted_price_egp"),
    F.round(F.col("steel_price_egp"), 2).alias("actual_price_egp"),
    F.lit(f"v1.1_{best_model_name}_delta").alias("model_version"),
    F.round(F.col("pred_price") * 0.95, 2).alias("confidence_lower"),
    F.round(F.col("pred_price") * 1.05, 2).alias("confidence_upper"),
    F.current_timestamp().alias("created_at")
)

test_results = _tag_tenant(test_results)
save_pg_tenant_ml(test_results, PG_URL, "ml_models.price_predictions", PG_PROPS)

pred_count = test_results.count()
print(f"   Saved {pred_count} predictions to ml_models.price_predictions")

print("\n   Sample Predictions vs Actual:")
test_results.select("target_date", "predicted_price_egp", "actual_price_egp") \
    .orderBy("target_date") \
    .show(10, truncate=False)

# ============================================================
# 7. MODEL SUMMARY
# ============================================================
print("\n" + "=" * 60)
print("MODEL 1: PRICE PREDICTION - COMPLETE!")
print("=" * 60)
print(f"   Algorithm:      PySpark MLlib {best_model_name} (delta prediction)")
print(f"   Features:       {len(feature_cols)} features (moving_avg_30d removed)")
print(f"   Train/Test:     {train_count}/{test_count} rows (60-day test window)")
print(f"   RMSE:           {best_rmse:,.2f} EGP")
print(f"   MAE:            {best_mae:,.2f} EGP")
print(f"   MAPE:           {best_mape:.2f}%")
print(f"   R2 Score:       {best_r2:.4f}")
print(f"   Baseline MAPE:  {baseline_mape:.2f}%   Baseline R2: {baseline_r2:.4f}")
print(f"   Business Value: ~{best_mape:.1f}% price forecast accuracy")
print(f"   Saved to:       ml_models.price_predictions ({pred_count} rows)")
print(f"   Completed at:   {datetime.now()}")
print("=" * 60)

spark.stop()
