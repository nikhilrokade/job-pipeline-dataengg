from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

location_schema = StructType([StructField("name", StringType(), True)])

raw_payload_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("location", location_schema, True),
        StructField("absolute_url", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("first_published", StringType(), True),
    ]
)

record_schema = StructType(
    [
        StructField("dedup_fingerprint", StringType(), True),
        StructField("company_board", StringType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at_utc", StringType(), True),
        StructField("raw_payload", raw_payload_schema, True),
    ]
)

envelope_schema = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("source_pipeline", StringType(), True),
                    StructField("ingested_at_utc", StringType(), True),
                    StructField("record_count", IntegerType(), True),
                    StructField(
                        "companies_scanned", ArrayType(StringType()), True
                    ),
                ]
            ),
            True,
        ),
        StructField("records", ArrayType(record_schema), True),
    ]
)