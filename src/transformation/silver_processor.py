import glob
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.transformation.schemas import envelope_schema


def process_silver_layer(spark: SparkSession, bronze_dir: str) -> int:
    bronze_files = glob.glob(os.path.join(bronze_dir, "*.json"))
    if not bronze_files:
        raise FileNotFoundError(f"No Bronze JSON files found in {bronze_dir}")

    all_batches = []
    for filepath in bronze_files:
        with open(filepath, "r", encoding="utf-8") as f:
            all_batches.append(json.load(f))

    raw_bronze_df = spark.createDataFrame(all_batches, schema=envelope_schema)

    exploded_df = raw_bronze_df.select(
        F.col("metadata.ingested_at_utc").alias("batch_ingested_at"),
        F.explode(F.col("records")).alias("job"),
    )

    silver_clean_df = (
        exploded_df.select(
            F.col("job.dedup_fingerprint").alias("job_fingerprint"),
            F.col("job.raw_payload.id")
            .cast("string")
            .alias("external_job_id"),
            F.lower(
                F.coalesce(
                    F.trim(F.col("job.raw_payload.company_name")),
                    F.trim(F.col("job.company_board")),
                )
            ).alias("company"),
            F.trim(F.col("job.raw_payload.title")).alias("title"),
            F.trim(F.col("job.raw_payload.location.name")).alias("location"),
            F.col("job.raw_payload.absolute_url").alias("job_url"),
            F.to_timestamp(F.col("job.raw_payload.updated_at")).alias(
                "source_updated_at"
            ),
            F.coalesce(
                F.to_timestamp(F.col("job.ingested_at_utc")),
                F.to_timestamp(F.col("batch_ingested_at")),
            ).alias("ingested_at_utc"),
            F.col("job.source").alias("source_system"),
        )
        .filter(F.col("job_fingerprint").isNotNull())
        .dropDuplicates(["job_fingerprint"])
    )

    silver_enriched_df = silver_clean_df.withColumn(
        "is_remote",
        F.when(
            F.lower(F.col("location")).like("%remote%")
            | F.lower(F.col("location")).like("%home based%")
            | F.lower(F.col("location")).like("%worldwide%")
            | F.lower(F.col("title")).like("%remote%"),
            True,
        ).otherwise(False),
    )

    (
        silver_enriched_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("company")
        .saveAsTable("jobs_silver")
    )

    return silver_enriched_df.count()