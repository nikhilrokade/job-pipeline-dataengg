from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process_gold_layer(spark: SparkSession):
    silver_df = spark.table("jobs_silver")

    # 1. Company metrics
    gold_company_metrics = (silver_df.groupBy("company").agg(
            F.count("job_fingerprint").alias("total_active_jobs"),
            F.sum(F.when(F.col("is_remote") == True, 1).otherwise(0)).alias(
                "remote_jobs"
            ),
            F.max("source_updated_at").alias("latest_posting_date"),
        )
        .withColumn(
            "remote_percentage",
            F.round(
                (F.col("remote_jobs") / F.col("total_active_jobs")) * 100, 2
            ),
        )
        .orderBy(F.col("total_active_jobs").desc())
    )

    gold_company_metrics.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("gold_company_metrics")

    # 2. Remote distribution
    gold_remote_distribution = (
        silver_df.groupBy("is_remote")
        .agg(F.count("job_fingerprint").alias("job_count"))
        .withColumn(
            "work_model",
            F.when(
                F.col("is_remote") == True, "Remote / Distributed"
            ).otherwise("On-site / Hybrid"),
        )
        .select("work_model", "job_count")
    )

    gold_remote_distribution.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("gold_remote_distribution")