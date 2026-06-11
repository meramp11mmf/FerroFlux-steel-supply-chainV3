# ============================================================
# PRODUCTION EFFICIENCY FIX — Scan + Fix + Save
# ============================================================
# Reads:  silver_clean parquet
# Fixes:  underperforming-batch annotation, zero-efficiency reset
#         net_output_tons derivation, loaded_at refresh
# Output: analytics.production_efficiency   (Gold — 1 row per line)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

print("=" * 60)
print("PRODUCTION EFFICIENCY FIX — Starting...")
print(f"Time: {datetime.now()}")
print("=" * 60)

# ===================== SparkSession ======================
spark = (SparkSession.builder
    .appName("Steel_Supply_Chain_ETL")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .config("spark.driver.extraJavaOptions",
            "-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.enable", "false")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .config("spark.hadoop.dfs.permissions.enabled", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    .config("spark.hadoop.parquet.enable.summary-metadata", "false")
    .config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate())

# ── TENANT SCOPE ─────────────────────────────────────────────
import os as _os
FF_COMPANY = _os.getenv("FF_COMPANY", "").strip()
FF_FACTORY = _os.getenv("FF_FACTORY", "").strip()
def _scope(df):
    if FF_COMPANY and "company_id" in df.columns:
        df = df.filter(df.company_id == FF_COMPANY)
    if FF_FACTORY and "factory_id" in df.columns:
        df = df.filter(df.factory_id == FF_FACTORY)
    return df
# ─────────────────────────────────────────────────────────────


spark.sparkContext.setLogLevel("WARN")

# ===================== Paths & Creds ======================
SILVER_PATH = "/opt/spark/data/processed/silver"
GOLD_PATH   = "/opt/spark/data/processed/gold"

# All PostgreSQL connection details sourced from env vars — no hardcoded credentials
_pg_host = _os.getenv("PG_HOST", "steel-postgres")
_pg_port = _os.getenv("PG_PORT", "5432")
_pg_db   = _os.getenv("PG_DB",   "steel_db")
_pg_user = _os.getenv("PG_USER", _os.getenv("POSTGRES_USER", "steel_admin"))
_pg_pass = _os.getenv("PG_PASSWORD", _os.getenv("POSTGRES_PASSWORD", ""))
PG_URL   = f"jdbc:postgresql://{_pg_host}:{_pg_port}/{_pg_db}"
PG_PROPS = {"user": _pg_user, "password": _pg_pass, "driver": "org.postgresql.Driver"}

# ======================= LOAD SILVER ======================
print("\n" + "=" * 50)
print("Loading silver layer data...")
print("=" * 50)

try:
    df_prod = _scope(spark.read.parquet(f"{SILVER_PATH}/production_clean"))
    total_before = df_prod.count()
    cols = len(df_prod.columns)
    print(f"   Read production_clean:  {total_before:,} rows  ({cols} columns)")
except Exception as e:
    print(f"   ERROR reading production_clean: {e}")
    spark.stop()
    raise

# ============ SECTION 1 — DIAGNOSTIC SCAN ==============
print("\n" + "=" * 50)
print("SECTION 1 — Efficiency Diagnostic Scan")
print("=" * 50)

# — Overall efficiency distribution —
stats = df_prod.select(
    F.min("efficiency_pct").alias("min_eff"),
    F.max("efficiency_pct").alias("max_eff"),
    F.avg("efficiency_pct").alias("avg_eff"),
    F.expr("percentile_approx(efficiency_pct, 0.05)").alias("p5"),
    F.expr("percentile_approx(efficiency_pct, 0.25)").alias("p25"),
    F.expr("percentile_approx(efficiency_pct, 0.50)").alias("p50"),
    F.expr("percentile_approx(efficiency_pct, 0.75)").alias("p75"),
    F.expr("percentile_approx(efficiency_pct, 0.95)").alias("p95")
).collect()[0]

print(f"   Efficiency stats (pct):")
print(f"      min={stats['min_eff']:.1f}%  p5={stats['p5']:.1f}%  "
      f"p25={stats['p25']:.1f}%  median={stats['p50']:.1f}%")
print(f"      avg={stats['avg_eff']:.1f}%  "
      f"p75={stats['p75']:.1f}%  p95={stats['p95']:.1f}%  max={stats['max_eff']:.1f}%")

# — Outlier categories —
LOW_THRESHOLD = 70.0

df_zero   = df_prod.filter((F.col("efficiency_pct") == 0) & (F.col("actual_tons") > 0))
df_neg    = df_prod.filter(F.col("efficiency_pct") < 0)
df_low    = df_prod.filter((F.col("efficiency_pct") < LOW_THRESHOLD) &
                            (F.col("efficiency_pct") > 0) &
                            (F.col("actual_tons") > 0))
df_zero_a = df_prod.filter((F.col("actual_tons") == 0) |
                            F.col("actual_tons").isNull())
df_high   = df_prod.filter(F.col("efficiency_pct") > 100)

print(f"\n   Outlier categories:")
print(f"      0% efficiency  (actual>0) : {df_zero.count():>7,}")
print(f"      negative efficiency       : {df_neg.count():>7,}")
print(f"      low  (<70%, actual>0)     : {df_low.count():>7,}")
print(f"      zero actual tons          : {df_zero_a.count():>7,}")
print(f"      over 100%                 : {df_high.count():>7,}")

# — Underperforming breakdown by facility and shift —
underperf_df = df_prod.filter(
    (F.col("efficiency_pct") < LOW_THRESHOLD) & (F.col("actual_tons") > 0)
)

up_by_facility = underperf_df.groupBy("company_id","factory_id","facility").agg(
    F.count("*").alias("bad_batches"),
    F.avg("efficiency_pct").alias("avg_eff_when_bad")
).orderBy(F.desc("bad_batches"))

print(f"\n   Underperforming batches (<70%) by facility:")
for r in up_by_facility.collect():
    print(f"      {r['facility']:>8}:  {r['bad_batches']:>6,} batches  "
          f"(avg eff at failure = {r['avg_eff_when_bad']:.1f}%)")

up_by_shift = underperf_df.groupBy("company_id","factory_id","shift").agg(
    F.count("*").alias("bad_batches"),
    F.avg("efficiency_pct").alias("avg_eff_when_bad")
).orderBy(F.desc("bad_batches"))

print(f"\n   Underperforming batches (<70%) by shift:")
for r in up_by_shift.collect():
    print(f"      {r['shift']:<12}:  {r['bad_batches']:>6,} batches  "
          f"(avg eff at failure = {r['avg_eff_when_bad']:.1f}%)")

# — Underperforming by production_line (top 5) —
up_by_line = underperf_df.groupBy("company_id","factory_id","production_line","facility").agg(
    F.count("*").alias("bad_batches"),
    F.min("efficiency_pct").alias("min_eff"),
    F.avg("efficiency_pct").alias("avg_eff")
).orderBy(F.desc("bad_batches")).limit(5)

print(f"\n   Top 5 underperforming production lines:")
for r in up_by_line.collect():
    print(f"      {r['production_line']:<24} ({r['facility']}) "
          f"batches={r['bad_batches']:>5,}  min_eff={r['min_eff']:.1f}%  avg={r['avg_eff']:.1f}%")

# =================== SECTION 2 — FIX ====================
print("\n" + "=" * 50)
print("SECTION 2 — Applying Production Efficiency Fixes")
print("=" * 50)

total_changes = 0
changes_a = 0
changes_b = 0
changes_c = 0
changes_d = 0

df_fixed = df_prod

# ── Fix A: Cap impossible efficiency values (>100%) ──
df_high_flag = df_fixed.filter(
    (F.col("efficiency_pct") > 100) & (F.col("actual_tons") > 0)
)
if df_high_flag.count() > 0:
    df_fixed = df_fixed.withColumn(
        "efficiency_pct",
        F.when(
            (F.col("efficiency_pct") > 100) & (F.col("actual_tons") > 0),
            F.round(
                F.least(F.col("actual_tons") / F.col("planned_tons") * 100,
                        F.lit(100.0)), 2
            )
        ).otherwise(F.col("efficiency_pct"))
    )
    changes_a = df_high_flag.count()
    total_changes += changes_a
    print(f"   [Fix A] Capped {changes_a:,} over-100% efficiencies to ≤100%"
          f"  (actual/planned)")

# ── Fix B: Clear is_underperforming for planned downtime rows ──
df_zero_a_flag = df_fixed.filter(
    (F.col("actual_tons") == 0) & (F.col("efficiency_pct") == 0)
)
if df_zero_a_flag.count() > 0:
    df_fixed = df_fixed.withColumn(
        "is_underperforming",
        F.when(
            (F.col("actual_tons") == 0) & (F.col("efficiency_pct") == 0),
            0   # maintenance / power_outage: not genuine underperformance
        ).otherwise(F.col("is_underperforming"))
    )
    changes_b = df_zero_a_flag.count()
    total_changes += changes_b
    print(f"   [Fix B] Cleared is_underperforming flag for "
          f"{changes_b:,} zero-production rows")

# ── Fix C: Re-derive net_output_tons (actual − waste, clamped ≥ 0) ──
df_fixed = df_fixed.withColumn(
    "net_output_tons",
    F.round(
        F.when(
            F.col("actual_tons").isNull() | F.col("waste_tons").isNull(),
            F.col("actual_tons")
        ).otherwise(
            F.greatest(F.col("actual_tons") - F.col("waste_tons"), F.lit(0.0))
        ), 2
    )
)
changes_c = df_prod.filter(
    F.col("net_output_tons") !=
    F.greatest(df_prod["actual_tons"] - df_prod["waste_tons"], F.lit(0.0))
).count()
if changes_c > 0:
    total_changes += changes_c
    print(f"   [Fix C] Re-derived net_output_tons for "
          f"{changes_c:,} rows (actual−waste, clamped ≥0)")

# ── Fix D: Clamp any remaining negative net_output_tons ──
df_neg_net = df_fixed.filter(F.col("net_output_tons") < 0)
if df_neg_net.count() > 0:
    df_fixed = df_fixed.withColumn(
        "net_output_tons",
        F.when(F.col("net_output_tons") < 0, F.lit(0.0))
        .otherwise(F.col("net_output_tons"))
    )
    changes_d = df_neg_net.count()
    total_changes += changes_d
    print(f"   [Fix D] Clamped {changes_d:,} negative net_output_tons → 0")

# ── Fix E: Recompute is_underperforming and shift_rank ──
shift_rank_expr = (
    F.when(F.col("shift") == "morning",   1)
     .when(F.col("shift") == "afternoon", 2)
     .when(F.col("shift") == "night",     3)
     .otherwise(4)
)

df_fixed = (df_fixed
    .withColumn("is_underperforming",
                F.when(F.col("efficiency_pct") < LOW_THRESHOLD, 1)
                 .otherwise(0))
    .withColumn("shift_rank", shift_rank_expr)
    .withColumn("loaded_at", F.current_timestamp())
)

# ── Break lineage: write to temp path, read back as fresh DataFrame ──
# This is the only reliable way to:
#   (a) avoid the overwrite-then-read-from-deleted-files race, and
#   (b) prevent Parquet assertion errors when Spark evicts cache blocks
#       and tries to recompute from the just-overwritten source files.
TEMP_PATH = f"{SILVER_PATH}/_production_clean_tmp"
print(f"\n   Writing to temp path to break lineage...")
df_fixed.write.mode("overwrite").parquet(TEMP_PATH)
df_fixed = spark.read.parquet(TEMP_PATH)   # fresh DataFrame — no stale plan
print(f"   Lineage broken. Temp parquet ready.")

# ============== SECTION 3 — POST-FIX AUDIT ==============
print("\n" + "=" * 50)
print("SECTION 3 — Post-Fix Audit Summary")
print("=" * 50)

total_after    = df_fixed.count()
removed_rows   = total_before - total_after
null_eff       = df_fixed.filter(F.col("efficiency_pct").isNull()).count()
neg_eff_after  = df_fixed.filter(F.col("efficiency_pct") < 0).count()
zero_act_after = df_fixed.filter(
    (F.col("actual_tons") == 0) | F.col("actual_tons").isNull()
).count()
up_before_count = underperf_df.count()
underperf_after = df_fixed.filter(
    (F.col("efficiency_pct") < LOW_THRESHOLD) & (F.col("actual_tons") > 0)
).count()

print(f"   Total rows before : {total_before:>8,}")
print(f"   Total rows after  : {total_after:>8,}  (removed={removed_rows})")
print(f"   Total changes     : {total_changes:>8,}")
print(f"\n   Post-fix efficiency health:")
print(f"       null values     : {null_eff:>7,}")
print(f"       negative values : {neg_eff_after:>7,}")
print(f"       zero-actual rows: {zero_act_after:>7,}")
print(f"\n   Underperforming batches (<70%, actual>0):")
print(f"       before fix : {up_before_count:>8,}")
print(f"       after fix  : {underperf_after:>8,}")

# ── Worst 10 underperforming batches after fix ──
bad_sample = (df_fixed
    .filter((F.col("efficiency_pct") < LOW_THRESHOLD) & (F.col("actual_tons") > 0))
    .orderBy("efficiency_pct")
    .limit(10)
    .select("batch_id", "date", "facility", "production_line",
            "shift", "actual_tons", "efficiency_pct", "is_underperforming")
    .collect()
)

print(f"\n   Worst 10 underperforming batches (post-fix):")
print(f"   {'batch_id':<22} {'date':<12} {'facility':>8} {'line':>22} "
      f"{'shift':<10} {'actual':>7} {'eff%':>6} {'flag':>5}")
print(f"   {'-'*94}")
for r in bad_sample:
    print(f"   {r['batch_id']:<22} {str(r['date']):<12} {r['facility']:>8} "
          f"{r['production_line']:>22} {r['shift']:<10} "
          f"{r['actual_tons']:>6.0f} {r['efficiency_pct']:>5.1f}% "
          f"{r['is_underperforming']:>5}")

# ── Per-shift average efficiency ──
shift_summary = (df_fixed
    .groupBy("company_id","factory_id","shift")
    .agg(F.count("*").alias("batches"),
         F.avg("efficiency_pct").alias("avg_eff"),
         F.min("efficiency_pct").alias("min_eff"),
         F.max("efficiency_pct").alias("max_eff"))
    .orderBy("avg_eff", ascending=False)
)

print(f"\n   Shift efficiency (post-fix):")
for r in shift_summary.collect():
    bar = "█" * int(r["avg_eff"] // 5)
    print(f"      {r['shift']:<12} {bar:<20}  "
          f"avg={r['avg_eff']:.1f}%  [{r['min_eff']:.0f}%–{r['max_eff']:.0f}%]  "
          f"({r['batches']:,} batches)")

# ── Per-facility average efficiency ──
fac_summary = (df_fixed
    .groupBy("company_id","factory_id","facility")
    .agg(F.count("*").alias("batches"),
         F.avg("efficiency_pct").alias("avg_eff"),
         F.min("efficiency_pct").alias("min_eff"),
         F.max("efficiency_pct").alias("max_eff"))
    .orderBy("avg_eff", ascending=False)
)

print(f"\n   Facility efficiency (post-fix):")
for r in fac_summary.collect():
    print(f"      {r['facility']:<8}  avg={r['avg_eff']:.1f}%  "
          f"[{r['min_eff']:.0f}%–{r['max_eff']:.0f}%]  ({r['batches']:,} batches)")

# ============ SECTION 4 — RESAVE TO SILVER (fixed) ===============
print("\n" + "=" * 50)
print("SECTION 4 — Resaving Corrected Data")
print("=" * 50)

# df_fixed now reads from TEMP_PATH — safe to overwrite original
df_fixed.write.partitionBy("company_id","factory_id").mode("overwrite").option("partitionOverwriteMode","dynamic").parquet(f"{SILVER_PATH}/production_clean")
print(f"   Silver parquet overwritten : {SILVER_PATH}/production_clean")
# NOTE: TEMP_PATH is intentionally NOT deleted here.
# All gold computations below still lazily read from TEMP_PATH.
# Cleanup happens after all writes are complete (see end of script).

# ── Build Gold KOI aggregation ──
prod_eff_gold = (df_fixed
    .groupBy("production_line", "facility", "line_type")
    .agg(
        F.sum("actual_tons").alias("total_output_tons"),
        F.avg("efficiency_pct").alias("avg_efficiency"),
        F.avg("waste_tons").alias("avg_waste_pct"),
        F.avg("quality_score").alias("avg_quality_score"),
        F.avg(
            F.when(F.col("actual_tons") > 0,
                   F.col("energy_kwh") / F.col("actual_tons"))
            .otherwise(None)
        ).alias("energy_per_ton_kwh"),
        F.avg(
            F.when(F.col("actual_tons") > 0,
                   F.col("natural_gas_m3") / F.col("actual_tons"))
            .otherwise(None)
        ).alias("gas_per_ton_m3"),
        F.count("batch_id").alias("total_batches"),
        F.round(
            F.sum(
                F.when(F.col("status").isin("maintenance", "power_outage"), 1)
                .otherwise(0)
            ) / F.count("batch_id") * 100, 2
        ).alias("downtime_pct")
    )
)

# ── Best / worst shift per production line ──
shift_rank_expr2 = (
    F.when(F.col("shift") == "morning",   1)
     .when(F.col("shift") == "afternoon", 2)
     .when(F.col("shift") == "night",     3)
     .otherwise(4)
)

df_fixed2 = df_fixed.withColumn("shift_rank", shift_rank_expr2)

w_line = Window.partitionBy("company_id","factory_id","production_line")
shift_by_eff2 = (df_fixed2
    .groupBy("production_line", "shift")
    .agg(F.avg("efficiency_pct").alias("avg_eff_shift"))
    .withColumn("line_avg", F.avg("avg_eff_shift").over(w_line))
)

best_s2 = (shift_by_eff2
    .withColumn("r", F.row_number().over(
        Window.partitionBy("company_id","factory_id","production_line").orderBy(F.desc("avg_eff_shift"))))
    .filter(F.col("r") == 1)
    .select("production_line", F.col("shift").alias("best_shift"))
)

worst_s2 = (shift_by_eff2
    .withColumn("r", F.row_number().over(
        Window.partitionBy("company_id","factory_id","production_line").orderBy(F.asc("avg_eff_shift"))))
    .filter(F.col("r") == 1)
    .select("production_line", F.col("shift").alias("worst_shift"))
)

prod_eff_gold2 = (prod_eff_gold
    .join(best_s2,  "production_line", "left")
    .join(worst_s2, "production_line", "left")
    .select(
        "production_line", "facility", "line_type",
        "total_output_tons", "avg_efficiency", "avg_waste_pct",
        "avg_quality_score", "energy_per_ton_kwh", "gas_per_ton_m3",
        "best_shift", "worst_shift", "total_batches", "downtime_pct"
    )
    .withColumn("loaded_at", F.current_timestamp())
)

gold_row_count = prod_eff_gold2.count()

prod_eff_gold2.write.partitionBy("company_id","factory_id","facility").mode("overwrite").option("partitionOverwriteMode","dynamic").parquet(f"{GOLD_PATH}/production_efficiency")
prod_eff_gold2.write.mode("overwrite").jdbc(
    PG_URL, "analytics.production_efficiency", properties=PG_PROPS
)
print(f"   Gold KOI updated    : analytics.production_efficiency "
      f"({gold_row_count} lines)")

# ===================== SUMMARY ==========================
print("\n" + "=" * 60)
print("PRODUCTION EFFICIENCY FIX — COMPLETE!")
print("=" * 60)
print(f"Total rows processed    : {total_before:>8,}")
print(f"Total changes applied   : {total_changes:>8,}")
print(f"Rows after fix          : {total_after:>8,}")
print(f"Remaining underperformers (<70%, actual>0): {underperf_after:>6,}")
print(f"")
print(f"Fixes applied:")
print(f"  [Fix A] Over-100% efficiency capped           : {changes_a:,}")
print(f"  [Fix B] Zero-production flag cleared          : {changes_b:,}")
print(f"  [Fix C] net_output_tons re-derived            : {changes_c:,}")
print(f"  [Fix D] Negative net_output_tons clamped      : {changes_d:,}")
print(f"")
print(f"Silver parquet  : production_clean  (overwritten)")
print(f"Gold KOI        : production_efficiency  ({gold_row_count} rows)")
print(f"PostgreSQL      : analytics schema  (updated)")
print(f"Completed at    : {datetime.now()}")
print("=" * 60)

# All Spark jobs done — now safe to delete the temp path
import shutil
try:
    shutil.rmtree(TEMP_PATH)
    print(f"Temp path cleaned up: {TEMP_PATH}")
except Exception as e:
    print(f"Temp cleanup skipped: {e}")

spark.stop()