# Fabric PySpark Notebook — Silver Demand (T3-1, T3-2)
# Attach: silver-lakehouse (default lakehouse for this notebook)
# Reads from bronze-lakehouse, SCD Type 1 upsert to silver.demand
#
# Transformations:
#   - Parse EIA period string ("2024-01-15T13") to UTC timestamp
#   - Cast value (MWh) -> demand_mwh; rename respondent columns
#   - Deduplicate: keep latest _ingested_at per (period_utc, respondent)
#   - Merge into silver.demand on (period_utc, respondent)

import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

# ── Paths ─────────────────────────────────────────────────────────────────────
try:
    from notebookutils import mssparkutils
    workspace_id = mssparkutils.runtime.context["currentWorkspaceId"]
except Exception:
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]

BRONZE_PATH = (
    f"abfss://bronze-lakehouse@onelake.dfs.fabric.microsoft.com"
    f"/{workspace_id}/bronze-lakehouse.Lakehouse/Tables"
)

# ── Read bronze ───────────────────────────────────────────────────────────────
raw = spark.read.format("delta").load(f"{BRONZE_PATH}/eia_demand_raw")
log.info("Bronze demand rows: %d", raw.count())

# ── Transform ─────────────────────────────────────────────────────────────────
cleaned = (
    raw
    # Parse EIA period format "2024-01-15T13" -> timestamp
    .withColumn(
        "period_utc",
        F.to_timestamp(F.col("period"), "yyyy-MM-dd'T'HH").cast(TimestampType()),
    )
    .withColumn("demand_mwh", F.col("value").cast(DoubleType()))
    # demand_mw for hourly data equals demand_mwh (1-hour intervals)
    .withColumn("demand_mw", F.col("demand_mwh"))
    .withColumn("_cleansed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    .select(
        "period_utc",
        "respondent",
        F.col("respondent_name").alias("respondent_name"),
        "demand_mwh",
        "demand_mw",
        "_ingested_at",
        "_cleansed_at",
    )
    .filter(F.col("period_utc").isNotNull() & F.col("demand_mwh").isNotNull())
)

# Deduplicate: keep latest ingestion per natural key
deduped = (
    cleaned
    .withColumn(
        "_rn",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("period_utc", "respondent")
            .orderBy(F.col("_ingested_at").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
log.info("Deduplicated demand rows: %d", deduped.count())

# ── SCD Type 1 upsert ─────────────────────────────────────────────────────────
silver_table = "demand"
silver_path = f"Tables/{silver_table}"

try:
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, silver_path):
        dt = DeltaTable.forPath(spark, silver_path)
        (
            dt.alias("t")
            .merge(deduped.alias("s"), "t.period_utc = s.period_utc AND t.respondent = s.respondent")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("Merged into silver.%s", silver_table)
    else:
        deduped.write.format("delta").save(silver_path)
        log.info("Created silver.%s", silver_table)
except Exception as exc:
    log.warning("Delta merge not available (%s) — falling back to overwrite-dedup", exc)
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
