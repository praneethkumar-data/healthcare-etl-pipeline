"""
Silver → Gold Transformation
Builds star schema in Snowflake from Silver Delta tables.
"""

import logging
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class SilverToGoldTransform:
    """
    Loads Snowflake star schema from Silver Delta layer.

    Builds:
    - fact_claims
    - dim_member
    - dim_provider
    - dim_date
    - dim_diagnosis
    """

    def __init__(self, execution_date: datetime, config: Dict[str, Any] = None):
        self.execution_date = execution_date
        self.config = config or self._default_config()
        self.spark = self._get_spark()

    def _default_config(self) -> Dict[str, Any]:
        return {
            "silver_path": "data/delta/silver/claims",
            "snowflake": {
                "url":        "your-account.snowflakecomputing.com",
                "database":   "HEALTHCARE_DW",
                "schema":     "GOLD",
                "warehouse":  "COMPUTE_WH",
                "role":       "DATA_ENGINEER",
            },
        }

    def _get_spark(self) -> SparkSession:
        sf = self.config["snowflake"]
        return (
            SparkSession.builder
            .appName("SilverToGold")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.jars.packages",
                    "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4")
            .getOrCreate()
        )

    def _snowflake_options(self) -> Dict[str, str]:
        sf = self.config["snowflake"]
        return {
            "sfURL":       sf["url"],
            "sfDatabase":  sf["database"],
            "sfSchema":    sf["schema"],
            "sfWarehouse": sf["warehouse"],
            "sfRole":      sf["role"],
            "sfUser":      __import__("os").environ.get("SNOWFLAKE_USER", ""),
            "sfPassword":  __import__("os").environ.get("SNOWFLAKE_PASSWORD", ""),
        }

    def _write_to_snowflake(self, df: DataFrame, table: str, mode: str = "overwrite"):
        """Write a DataFrame to a Snowflake table."""
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**self._snowflake_options())
            .option("dbtable", table)
            .mode(mode)
            .save()
        )
        logger.info(f"Written to Snowflake: {table} ({df.count()} rows)")

    # ─── Dimension Builders ──────────────────────────────────────────────────

    def build_dim_date(self, df: DataFrame) -> DataFrame:
        """Build date dimension from service dates."""
        return (
            df.select("service_date").distinct()
            .withColumn("date_key",       F.date_format("service_date", "yyyyMMdd").cast("int"))
            .withColumn("full_date",      F.col("service_date"))
            .withColumn("year",           F.year("service_date"))
            .withColumn("quarter",        F.quarter("service_date"))
            .withColumn("month",          F.month("service_date"))
            .withColumn("month_name",     F.date_format("service_date", "MMMM"))
            .withColumn("week_of_year",   F.weekofyear("service_date"))
            .withColumn("day_of_week",    F.dayofweek("service_date"))
            .withColumn("day_name",       F.date_format("service_date", "EEEE"))
            .withColumn("is_weekend",     F.dayofweek("service_date").isin([1, 7]))
            .drop("service_date")
        )

    def build_dim_member(self, df: DataFrame) -> DataFrame:
        """Build member dimension (hashed PII)."""
        return (
            df.select("member_id", "payer_id").distinct()
            .withColumn("member_key",     F.md5(F.col("member_id")).cast("string"))
            .withColumn("member_id_hash", F.col("member_id"))
            .withColumn("payer_id",       F.col("payer_id"))
            .withColumn("_updated_at",    F.current_timestamp())
            .select("member_key", "member_id_hash", "payer_id", "_updated_at")
        )

    def build_dim_provider(self, df: DataFrame) -> DataFrame:
        """Build provider dimension."""
        return (
            df.select("provider_id").distinct()
            .withColumn("provider_key",     F.md5(F.col("provider_id")).cast("string"))
            .withColumn("provider_id_hash", F.col("provider_id"))
            .withColumn("_updated_at",      F.current_timestamp())
            .select("provider_key", "provider_id_hash", "_updated_at")
        )

    def build_dim_diagnosis(self, df: DataFrame) -> DataFrame:
        """Build ICD-10 diagnosis dimension."""
        return (
            df.select("diagnosis_code").distinct()
            .filter(F.col("diagnosis_code").isNotNull())
            .withColumn("diagnosis_key",   F.md5(F.col("diagnosis_code")))
            .withColumn("icd10_code",      F.col("diagnosis_code"))
            # In production, join to ICD-10 reference table for descriptions
            .withColumn("description",     F.lit("See ICD-10 reference"))
            .withColumn("category",        F.col("diagnosis_code").substr(1, 1))
        )

    def build_fact_claims(self, df: DataFrame) -> DataFrame:
        """Build fact_claims with foreign keys to dimensions."""
        return (
            df
            .withColumn("claim_key",      F.md5(F.col("claim_id")))
            .withColumn("member_key",     F.md5(F.col("member_id")))
            .withColumn("provider_key",   F.md5(F.col("provider_id")))
            .withColumn("date_key",       F.date_format("service_date", "yyyyMMdd").cast("int"))
            .withColumn("diagnosis_key",  F.md5(F.col("diagnosis_code")))
            .select(
                "claim_key",
                "claim_id",
                "member_key",
                "provider_key",
                "date_key",
                "diagnosis_key",
                "payer_id",
                "claim_type",
                "claim_status",
                "procedure_code",
                "billed_amount",
                "allowed_amount",
                "paid_amount",
                "discount_rate",
                "is_denied",
                "place_of_service",
                "_silver_processed_at",
                F.current_timestamp().alias("_gold_loaded_at"),
            )
        )

    # ─── Main Run ─────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        logger.info("Starting Silver → Gold transformation")

        df = (
            self.spark.read
            .format("delta")
            .load(self.config["silver_path"])
            .filter(F.col("claim_year") == self.execution_date.year)
            .filter(F.col("claim_month") == self.execution_date.month)
        )

        # Build and load dimensions
        self._write_to_snowflake(self.build_dim_date(df),      "DIM_DATE",      "overwrite")
        self._write_to_snowflake(self.build_dim_member(df),    "DIM_MEMBER",    "overwrite")
        self._write_to_snowflake(self.build_dim_provider(df),  "DIM_PROVIDER",  "overwrite")
        self._write_to_snowflake(self.build_dim_diagnosis(df), "DIM_DIAGNOSIS", "overwrite")

        # Build and load fact table
        fact_df = self.build_fact_claims(df)
        self._write_to_snowflake(fact_df, "FACT_CLAIMS", "overwrite")

        return {
            "fact_records": fact_df.count(),
            "status": "SUCCESS",
            "execution_date": str(self.execution_date),
        }
