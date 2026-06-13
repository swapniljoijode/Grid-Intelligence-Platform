# Fabric PySpark Notebook — Silver Carbon Intensity (T3-1, T3-2)
# Attach: silver-lakehouse (default lakehouse for this notebook)
# Reads from bronze-lakehouse, SCD Type 1 upsert to silver.carbon_intensity
#
# Transformations:
#   - Parse ISO-8601 from/to strings to UTC timestamps
#   - Resolve actual vs forecast intensity; compute duration_minutes
#   - Deduplicate by (period_from, period_to)
#   - Merge into silver.carbon_intensity

import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

try:
    from notebookutils import mssparkutils
    workspace_id = mssparkutils.runtime.context["currentWorkspaceId"]
except Exception:
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]

BRONZE_PATH = (
    f"abfss://bronze-lakehouse@onelake.dfs.fabric.microsoft.com"
    f"/{workspace_id}/bronze-lakehouse.Lakehouse/Tables"
)

raw = spark.read.format("delta").load(f"{BRONZE_PATH}/carbon_intensity_raw")
log.info("Bronze carbon intensity rows: %d", raw.count())

cleaned = (
    raw
    # Parse ISO-8601 half-hourly timestamps  "2024-01-15T12:30Z" -> timestamp
    .withColumn("period_from", F.to_timestamp(F.col("from"), "yyyy-MM-dd'T'HH:mm'Z'").cast(TimestampType()))
    .withColumn("period_to", F.to_timestamp(F.col("to"), "yyyy-MM-dd'T'HH:mm'Z'").cast(TimestampType()))
    # Prefer actual intensity; fall back to forecast
    .withColumn(
        "intensity_gco2_kwh",
        F.coalesce(
            F.col("intensity_actual").cast(DoubleType()),
            F.col("intensity_forecast").cast(DoubleType()),
        ),
    )
    .withColumn(
        "is_estimated",
        F.col("intensity_actual").isNull().cast("boolean"),
    )
    # Duration in minutes (should always be 30 for this API)
    .withColumn(
        "duration_minutes",
        (F.unix_timestamp("period_to") - F.unix_timestamp("period_from")) / 60,
    )
    .withColumn("_cleansed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    .select(
        "period_from",
        "period_to",
        "intensity_gco2_kwh",
        F.col("intensity_index").alias("intensity_index"),
        "is_estimated",
        "duration_minutes",
        "_ingested_at",
        "_cleansed_at",
    )
    .filter(F.col("period_from").isNotNull() & F.col("intensity_gco2_kwh").isNotNull())
)

deduped = (
    cleaned
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("period_from", "period_to")
            .orderBy(F.col("_ingested_at").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
log.info("Deduplicated CI rows: %d", deduped.count())

silver_table = "carbon_intensity"
silver_path = f"Tables/{silver_table}"

try:
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, silver_path):
        dt = DeltaTable.forPath(spark, silver_path)
        (
            dt.alias("t")
            .merge(
                deduped.alias("s"),
                "t.period_from = s.period_from AND t.period_to = s.period_to",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("Merged into silver.%s", silver_table)
    else:
        deduped.write.format("delta").save(silver_path)
        log.info("Created silver.%s", silver_table)
except Exception as exc:
    log.warning("Delta merge not available (%s) — overwriting", exc)
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
