# ============================================================
# MODEL 3: Supplier Risk Scoring using PySpark MLlib
# ============================================================
# Algorithm: Random Forest Classifier + Scoring
# Input: raw_materials + supplier_scorecard
# Output: Risk score 0-100 per supplier with risk factors
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from datetime import datetime

print("=" * 60)
print("MODEL 3: Supplier Risk Scoring")
print(f"Time: {datetime.now()}")
print("=" * 60)

spark = SparkSession.builder \
    .appName("Steel_Supplier_Risk") \
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
print("\n1. Loading data from PostgreSQL...")

df_rawmat = _train_scope(spark.read.jdbc(PG_URL, "processed_data.rawmat_clean", properties=PG_PROPS))
df_scorecard = _train_scope(spark.read.jdbc(PG_URL, "analytics.supplier_scorecard", properties=PG_PROPS))

print(f"   Raw materials: {df_rawmat.count():,} rows")
print(f"   Supplier scorecard: {df_scorecard.count()} suppliers")

# ============================================================
# 2. FEATURE ENGINEERING - Per Delivery Level
# ============================================================
print("\n2. Building delivery-level risk features...")

# Quality grade to numeric
df_rawmat = df_rawmat \
    .withColumn("quality_num",
        F.when(F.col("quality_grade") == "A", 4.0)
        .when(F.col("quality_grade") == "B", 3.0)
        .when(F.col("quality_grade") == "C", 2.0)
        .when(F.col("quality_grade") == "D", 1.0)
        .otherwise(2.5)
    )

# Create risk label per delivery
# High risk = late delivery + low quality + unreliable
df_deliveries = df_rawmat \
    .withColumn("is_late", F.when(F.col("on_time") == 0, 1).otherwise(0)) \
    .withColumn("is_low_quality", F.when(F.col("quality_num") <= 2.0, 1).otherwise(0)) \
    .withColumn("is_expensive",
        F.when(F.col("price_per_ton_usd") > 
            F.avg("price_per_ton_usd").over(Window.partitionBy("material_type")), 1
        ).otherwise(0)
    ) \
    .withColumn("risk_points",
        F.col("is_late") * 40 +
        F.col("is_low_quality") * 30 +
        F.col("is_expensive") * 15 +
        F.when(F.col("days_late") > 7, 15).otherwise(F.col("days_late") * 2)
    ) \
    .withColumn("risk_label",
        F.when(F.col("risk_points") >= 60, 2)  # HIGH
        .when(F.col("risk_points") >= 30, 1)    # MEDIUM
        .otherwise(0)                             # LOW
    )

# Show risk distribution
print("   Risk distribution:")
df_deliveries.groupBy("risk_label").count().orderBy("risk_label").show()

# ============================================================
# 3. PREPARE FEATURES FOR ML
# ============================================================
print("3. Preparing features...")

# Encode supplier and material type
supplier_indexer = StringIndexer(inputCol="supplier_name", outputCol="supplier_idx")
material_indexer = StringIndexer(inputCol="material_type", outputCol="material_idx")
country_indexer = StringIndexer(inputCol="origin_country", outputCol="country_idx")

feature_cols = [
    "quantity_tons", "price_per_ton_usd", "ocean_freight_usd_per_ton",
    "total_landed_cost_usd", "lead_time_days", "days_late",
    "quality_num", "supplier_reliability",
    "supplier_idx", "material_idx", "country_idx"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

# ============================================================
# 4. TRAIN CLASSIFIER (Risk Level Prediction)
# ============================================================
print("\n4. Training Risk Classification model...")

# Split 80/20
train_df, test_df = df_deliveries.randomSplit([0.8, 0.2], seed=42)
print(f"   Train: {train_df.count():,} rows, Test: {test_df.count():,} rows")

rf_classifier = RandomForestClassifier(
    featuresCol="features",
    labelCol="risk_label",
    numTrees=100,
    maxDepth=6,
    seed=42
)

pipeline_clf = Pipeline(stages=[
    supplier_indexer, material_indexer, country_indexer,
    assembler, scaler, rf_classifier
])

model_clf = pipeline_clf.fit(train_df)
predictions_clf = model_clf.transform(test_df)

# Evaluate
evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="risk_label", predictionCol="prediction", metricName="accuracy"
)
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="risk_label", predictionCol="prediction", metricName="f1"
)

accuracy = evaluator_acc.evaluate(predictions_clf)
f1_score = evaluator_f1.evaluate(predictions_clf)

print(f"   Classification Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
print(f"   F1 Score: {f1_score:.4f}")

# Confusion matrix
print("\n   Predictions vs Actual:")
predictions_clf.groupBy("risk_label", "prediction") \
    .count() \
    .orderBy("risk_label", "prediction") \
    .show()

# ============================================================
# 5. TRAIN REGRESSOR (Continuous Risk Score)
# ============================================================
print("5. Training Risk Score Regression model...")

rf_regressor = RandomForestRegressor(
    featuresCol="features",
    labelCol="risk_points",
    numTrees=100,
    maxDepth=6,
    seed=42
)

pipeline_reg = Pipeline(stages=[
    supplier_indexer, material_indexer, country_indexer,
    assembler, scaler, rf_regressor
])

model_reg = pipeline_reg.fit(train_df)
predictions_reg = model_reg.transform(test_df)

evaluator_rmse = RegressionEvaluator(labelCol="risk_points", predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="risk_points", predictionCol="prediction", metricName="r2")

rmse = evaluator_rmse.evaluate(predictions_reg)
r2 = evaluator_r2.evaluate(predictions_reg)

print(f"   Risk Score RMSE: {rmse:.2f} points")
print(f"   Risk Score R2:   {r2:.4f}")

# ============================================================
# 6. FEATURE IMPORTANCE
# ============================================================
print("\n6. Feature Importance (Classifier):")

rf_model = model_clf.stages[-1]
importances = rf_model.featureImportances.toArray()

for i, feat in enumerate(feature_cols):
    if i < len(importances):
        print(f"   {i+1}. {feat}: {importances[i]:.4f}")

# ============================================================
# 7. GENERATE SUPPLIER RISK SCORES
# ============================================================
print("\n7. Generating final supplier risk scores...")

# Score ALL deliveries with the regression model
all_scored = model_reg.transform(df_deliveries)

# Aggregate per supplier
supplier_risk = all_scored \
    .groupBy("supplier_name") \
    .agg(
        F.first("origin_country").alias("origin_country"),
        F.first("material_type").alias("material_type"),
        F.round(F.avg("prediction"), 2).alias("avg_risk_score"),
        F.round(F.avg("risk_points"), 2).alias("actual_avg_risk"),
        F.count("*").alias("total_deliveries"),
        F.round(F.avg(F.col("is_late").cast("double")) * 100, 2).alias("late_delivery_pct"),
        F.round(F.avg("quality_num"), 2).alias("avg_quality"),
        F.round(F.avg("lead_time_days"), 1).alias("avg_lead_time"),
        F.round(F.avg("price_per_ton_usd"), 2).alias("avg_price_usd"),
        F.round(F.avg("supplier_reliability"), 4).alias("avg_reliability")
    ) \
    .withColumn("on_time_factor", F.round(100 - F.col("late_delivery_pct"), 2)) \
    .withColumn("quality_factor", F.round(F.col("avg_quality") / 4 * 100, 2)) \
    .withColumn("price_factor", F.round(
        F.when(F.col("avg_price_usd") > 200, 40.0)
        .when(F.col("avg_price_usd") > 100, 60.0)
        .when(F.col("avg_price_usd") > 50, 80.0)
        .otherwise(90.0), 2
    )) \
    .orderBy("avg_risk_score")

# Data-driven risk_level thresholds (percentile-based)
score_percentiles = supplier_risk.agg(
    F.expr("percentile_approx(avg_risk_score, 0.33)").alias("p33"),
    F.expr("percentile_approx(avg_risk_score, 0.67)").alias("p67"),
).collect()[0]

thresh_low  = float(score_percentiles["p33"])
thresh_high = float(score_percentiles["p67"])
print(f"\n   Risk thresholds — LOW < {thresh_low:.1f} ≤ MEDIUM < {thresh_high:.1f} ≤ HIGH")

supplier_risk = supplier_risk.withColumn("risk_level",
    F.when(F.col("avg_risk_score") >= thresh_high, "HIGH")
    .when(F.col("avg_risk_score") >= thresh_low,   "MEDIUM")
    .otherwise("LOW")
)

print("\n   SUPPLIER RISK SCORECARD:")
supplier_risk.select(
    "supplier_name", "origin_country", "risk_level",
    "avg_risk_score", "late_delivery_pct", "avg_quality",
    "avg_lead_time", "total_deliveries"
).show(20, truncate=False)

# ============================================================
# 8. SAVE TO POSTGRESQL
# ============================================================
print("8. Saving risk scores to PostgreSQL...")

risk_results = supplier_risk.select(
    F.current_date().alias("scored_date"),
    "supplier_name",
    F.round(F.col("avg_risk_score"), 2).alias("risk_score"),
    "risk_level",
    "on_time_factor",
    "quality_factor",
    "price_factor",
    F.lit("v1.0_RandomForest").alias("model_version"),
    F.current_timestamp().alias("created_at")
)

risk_results = _tag_tenant(risk_results)
save_pg_tenant_ml(risk_results, PG_URL, "ml_models.supplier_risk_scores", PG_PROPS)

cnt = risk_results.count()
print(f"   Saved {cnt} supplier risk scores to ml_models.supplier_risk_scores")

# Show final results
print("\n   Final Risk Scores:")
risk_results.select("supplier_name", "risk_score", "risk_level",
    "on_time_factor", "quality_factor", "price_factor") \
    .orderBy("risk_score") \
    .show(20, truncate=False)

# ============================================================
# 9. RISK SUMMARY & RECOMMENDATIONS
# ============================================================
print("\n" + "=" * 60)
print("SUPPLIER RISK ANALYSIS - KEY FINDINGS")
print("=" * 60)

low_risk = supplier_risk.filter(F.col("risk_level") == "LOW").count()
med_risk = supplier_risk.filter(F.col("risk_level") == "MEDIUM").count()
high_risk = supplier_risk.filter(F.col("risk_level") == "HIGH").count()

print(f"   LOW Risk Suppliers:    {low_risk}")
print(f"   MEDIUM Risk Suppliers: {med_risk}")
print(f"   HIGH Risk Suppliers:   {high_risk}")

# Top 3 safest
print("\n   TOP 3 SAFEST Suppliers:")
safe = supplier_risk.orderBy("avg_risk_score").limit(3).collect()
for i, row in enumerate(safe):
    print(f"   {i+1}. {row['supplier_name']} ({row['origin_country']}) - Score: {row['avg_risk_score']}")

# Top 3 riskiest
print("\n   TOP 3 RISKIEST Suppliers:")
risky = supplier_risk.orderBy(F.desc("avg_risk_score")).limit(3).collect()
for i, row in enumerate(risky):
    print(f"   {i+1}. {row['supplier_name']} ({row['origin_country']}) - Score: {row['avg_risk_score']}")

# ============================================================
# 10. MODEL SUMMARY
# ============================================================
print("\n" + "=" * 60)
print("MODEL 3: SUPPLIER RISK SCORING - COMPLETE!")
print("=" * 60)
print(f"   Classification:")
print(f"     Algorithm:   PySpark MLlib RandomForest Classifier")
print(f"     Accuracy:    {accuracy*100:.2f}%")
print(f"     F1 Score:    {f1_score:.4f}")
print(f"   Risk Regression:")
print(f"     Algorithm:   PySpark MLlib RandomForest Regressor")
print(f"     RMSE:        {rmse:.2f} points")
print(f"     R2 Score:    {r2:.4f}")
print(f"   Features:      {len(feature_cols)} features")
print(f"   Suppliers:     {cnt} scored")
print(f"   Risk Levels:   {low_risk} LOW, {med_risk} MEDIUM, {high_risk} HIGH")
print(f"   Business Value: Supply chain resilience & risk mitigation")
print(f"   Saved to:      ml_models.supplier_risk_scores ({cnt} rows)")
print(f"   Completed at:  {datetime.now()}")
print("=" * 60)

spark.stop()
