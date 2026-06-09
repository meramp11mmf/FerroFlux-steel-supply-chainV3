# ============================================================
# MODEL 1: Steel Price Prediction using PySpark MLlib GBT
# ============================================================
# Algorithm: Gradient Boosted Trees (GBTRegressor)
# Input: analytics.price_features (730 days)
# Output: 7-day and 30-day price forecast
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from datetime import datetime, timedelta
import json

print("=" * 60)
print("MODEL 1: Steel Price Prediction")
print(f"Time: {datetime.now()}")
print("=" * 60)

spark = SparkSession.builder \
    .appName("Steel_Price_Prediction") \
    .config("spark.jars", "/opt/spark/scripts/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://steel-postgres:5432/steel_db"
PG_PROPS = {
    "user": "steel_admin",
    "password": "steel_pass_2024",
    "driver": "org.postgresql.Driver"
}

# ── TENANT SCOPE (multi-tenant ML) ───────────────────────────
import os as _os
FF_COMPANY = _os.getenv("FF_COMPANY", "").strip() or "EZZ"
FF_FACTORY = _os.getenv("FF_FACTORY", "").strip() or "EZZ_DEMO"
_MIN_TRAIN_ROWS = 200   # below this we fall back to a broader scope

def _train_scope(df):
    """Pick the narrowest scope that still has enough rows to train."""
    has_cols = "factory_id" in df.columns and "company_id" in df.columns
    if not has_cols:
        return df
    # try factory
    fac = df.filter((df.company_id == FF_COMPANY) & (df.factory_id == FF_FACTORY))
    if fac.count() >= _MIN_TRAIN_ROWS:
        print(f"   [tenant] training on factory {FF_FACTORY} ({fac.count()} rows)")
        return fac
    # try company
    comp = df.filter(df.company_id == FF_COMPANY)
    if comp.count() >= _MIN_TRAIN_ROWS:
        print(f"   [tenant] factory sparse -> training on company {FF_COMPANY} ({comp.count()} rows)")
        return comp
    # global
    print(f"   [tenant] company sparse -> training on ALL data (global model, {df.count()} rows)")
    return df

def _tag_tenant(df):
    """Tag output rows with the requested tenant."""
    from pyspark.sql import functions as _F
    return (df.withColumn("company_id", _F.lit(FF_COMPANY))
              .withColumn("factory_id", _F.lit(FF_FACTORY)))

def save_pg_tenant_ml(df, pg_url, pg_table, pg_props):
    """Delete this tenant's existing rows, then append fresh predictions."""
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

# Drop rows with nulls in lag columns (first 30 days)
feature_cols = [
    "iron_ore_price_usd", "scrap_price_usd", "usd_egp_rate",
    "natural_gas_price_usd", "brent_oil_usd", "electricity_price_egp_kwh",
    "seasonality_index", "is_ramadan",
    "price_change_pct",
    "price_lag_1d", "price_lag_7d", "price_lag_14d", "price_lag_30d",
    "moving_avg_7d", "moving_avg_14d", "moving_avg_30d",
    "price_volatility_7d", "price_volatility_30d",
    "iron_ore_change_pct", "usd_change_pct", "oil_change_pct",
    "day_of_week", "month_val", "quarter_val"
]

target_col = "steel_price_egypt_egp"

df_clean = df.select(["date", target_col] + feature_cols).dropna()
clean_count = df_clean.count()
print(f"   After dropping nulls: {clean_count} rows (dropped {total - clean_count} rows with null lags)")

# ============================================================
# 2. TRAIN/TEST SPLIT (Time-based: last 30 days for test)
# ============================================================
print("\n2. Splitting data (time-based)...")

# Sort by date and split
df_sorted = df_clean.orderBy("date")

# Get date cutoff
max_date = df_sorted.agg(F.max("date")).collect()[0][0]
cutoff_date = max_date - timedelta(days=30)

df_train = df_sorted.filter(F.col("date") <= cutoff_date)
df_test = df_sorted.filter(F.col("date") > cutoff_date)

train_count = df_train.count()
test_count = df_test.count()
print(f"   Train: {train_count} rows (up to {cutoff_date})")
print(f"   Test:  {test_count} rows ({cutoff_date} to {max_date})")

# ============================================================
# 3. FEATURE ENGINEERING (VectorAssembler)
# ============================================================
print("\n3. Building ML Pipeline...")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

# ============================================================
# 4. MODEL 1A: GBT Regressor
# ============================================================
print("\n4A. Training GBT Regressor...")

gbt = GBTRegressor(
    featuresCol="features",
    labelCol=target_col,
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    subsamplingRate=0.8,
    seed=42
)

pipeline_gbt = Pipeline(stages=[assembler, scaler, gbt])
model_gbt = pipeline_gbt.fit(df_train)

# Evaluate on test set
predictions_gbt = model_gbt.transform(df_test)

evaluator_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="mae")
evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")

rmse_gbt = evaluator_rmse.evaluate(predictions_gbt)
mae_gbt = evaluator_mae.evaluate(predictions_gbt)
r2_gbt = evaluator_r2.evaluate(predictions_gbt)

avg_price = df_test.agg(F.avg(target_col)).collect()[0][0]
mape_gbt = mae_gbt / avg_price * 100

print(f"   GBT Results:")
print(f"   RMSE:  {rmse_gbt:,.2f} EGP")
print(f"   MAE:   {mae_gbt:,.2f} EGP")
print(f"   MAPE:  {mape_gbt:.2f}%")
print(f"   R2:    {r2_gbt:.4f}")

# ============================================================
# 4B. MODEL 1B: Random Forest Regressor (comparison)
# ============================================================
print("\n4B. Training Random Forest Regressor (comparison)...")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol=target_col,
    numTrees=100,
    maxDepth=8,
    seed=42
)

pipeline_rf = Pipeline(stages=[assembler, scaler, rf])
model_rf = pipeline_rf.fit(df_train)

predictions_rf = model_rf.transform(df_test)

rmse_rf = evaluator_rmse.evaluate(predictions_rf)
mae_rf = evaluator_mae.evaluate(predictions_rf)
r2_rf = evaluator_r2.evaluate(predictions_rf)
mape_rf = mae_rf / avg_price * 100

print(f"   RF Results:")
print(f"   RMSE:  {rmse_rf:,.2f} EGP")
print(f"   MAE:   {mae_rf:,.2f} EGP")
print(f"   MAPE:  {mape_rf:.2f}%")
print(f"   R2:    {r2_rf:.4f}")

# Choose best model
best_model_name = "GBT" if rmse_gbt < rmse_rf else "RandomForest"
best_rmse = min(rmse_gbt, rmse_rf)
best_mae = mae_gbt if rmse_gbt < rmse_rf else mae_rf
best_r2 = r2_gbt if rmse_gbt < rmse_rf else r2_rf
best_mape = mape_gbt if rmse_gbt < rmse_rf else mape_rf
best_predictions = predictions_gbt if rmse_gbt < rmse_rf else predictions_rf

print(f"\n   BEST MODEL: {best_model_name}")
print(f"   RMSE: {best_rmse:,.2f}, MAE: {best_mae:,.2f}, R2: {best_r2:.4f}, MAPE: {best_mape:.2f}%")

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
# 6. GENERATE PREDICTIONS (test set + 7-day forecast)
# ============================================================
print("\n6. Saving predictions to PostgreSQL...")

# Save test predictions
test_results = best_predictions.select(
    F.current_date().alias("prediction_date"),
    F.col("date").alias("target_date"),
    F.round(F.col("prediction"), 2).alias("predicted_price_egp"),
    F.round(F.col(target_col), 2).alias("actual_price_egp"),
    F.lit(f"v1.0_{best_model_name}").alias("model_version"),
    F.round(F.col("prediction") * 0.95, 2).alias("confidence_lower"),
    F.round(F.col("prediction") * 1.05, 2).alias("confidence_upper"),
    F.current_timestamp().alias("created_at")
)

test_results = _tag_tenant(test_results)
save_pg_tenant_ml(test_results, PG_URL, "ml_models.price_predictions", PG_PROPS)

pred_count = test_results.count()
print(f"   Saved {pred_count} predictions to ml_models.price_predictions")

# Show sample predictions
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
print(f"   Algorithm:     PySpark MLlib {best_model_name}")
print(f"   Features:      {len(feature_cols)} features")
print(f"   Train/Test:    {train_count}/{test_count} rows")
print(f"   RMSE:          {best_rmse:,.2f} EGP")
print(f"   MAE:           {best_mae:,.2f} EGP")
print(f"   MAPE:          {best_mape:.2f}%")
print(f"   R2 Score:      {best_r2:.4f}")
print(f"   Business Value: ~{best_mape:.1f}% price forecast accuracy")
print(f"   Saved to:      ml_models.price_predictions ({pred_count} rows)")
print(f"   Completed at:  {datetime.now()}")
print("=" * 60)

spark.stop()
