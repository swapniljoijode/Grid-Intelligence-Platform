# Fabric PySpark Notebook — Silver Generation (T3-1, T3-2)
# Attach: silver-lakehouse (default lakehouse for this notebook)
# Reads from bronze-lakehouse, SCD Type 1 upsert to silver.generation
#
# Transformations:
#   - Parse EIA period string to UTC timestamp
#   - Cast value -> generation_mwh; normalise fuel_type codes
#   - Deduplicate by (period_utc, respondent, fuel_type)
#   - Merge into silver.generation

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

# EIA fuel type code → readable label
FUEL_TYPE_MAP = {
    "COL": "Coal", "NG": "Natural Gas", "NUC": "Nuclear",
    "OIL": "Oil", "WAT": "Hydro", "WND": "Wind",
    "SUN": "Solar", "OTH": "Other", "UNK": "Unknown",
}
fuel_map_expr = F.create_map([F.lit(x) for pair in FUEL_TYPE_MAP.items() for x in pair])

raw = spark.read.format("delta").load(f"{BRONZE_PATH}/eia_generation_raw")
log.info("Bronze generation rows: %d", raw.count())

cleaned = (
    raw
    .withColumn(
        "period_utc",
        F.to_timestamp(F.col("period"), "yyyy-MM-dd'T'HH").cast(TimestampType()),
    )
    .withColumn("generation_mwh", F.col("value").cast(DoubleType()))
    .withColumn(
        "fuel_type_label",
        F.coalesce(fuel_map_expr[F.col("fueltype")], F.col("fueltype")),
    )
    .withColumn("_cleansed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    .select(
        "period_utc",
        "respondent",
        F.col("fueltype").alias("fuel_type"),
        "fuel_type_label",
        "generation_mwh",
        "_ingested_at",
        "_cleansed_at",
    )
    .filter(
        F.col("period_utc").isNotNull()
        & F.col("generation_mwh").isNotNull()
        & F.col("generation_mwh").cast(DoubleType()).isNotNull()
    )
)

deduped = (
    cleaned
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("period_utc", "respondent", "fuel_type")
            .orderBy(F.col("_ingested_at").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
log.info("Deduplicated generation rows: %d", deduped.count())

silver_table = "generation"
silver_path = f"Tables/{silver_table}"

try:
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, silver_path):
        dt = DeltaTable.forPath(spark, silver_path)
        (
            dt.alias("t")
            .merge(
                deduped.alias("s"),
                "t.period_utc = s.period_utc AND t.respondent = s.respondent AND t.fuel_type = s.fuel_type",
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
