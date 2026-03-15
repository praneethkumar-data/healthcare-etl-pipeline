"""
Bronze → Silver Transformation
Cleans, validates, masks PII, and standardizes healthcare claims data.
"""

import logging
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class BronzeToSilverTransform:
    """
    Transforms raw Bronze claims data into clean Silver layer.

    Steps:
    1. Load from Bronze Delta table
    2. Apply schema validation
    3. Mask / hash PII fields (HIPAA compliance)
    4. Standardize codes (ICD-10, CPT, NPI)
    5. Validate business rules
    6. Deduplicate using CDC logic
    7. Write to Silver Delta table
    """

    PII_FIELDS = ["member_id", "provider_id"]  # Hash these for HIPAA

    VALID_CLAIM_TYPES = {"inpatient", "outpatient", "professional", "pharmacy"}
    VALID_STATUSES = {"paid", "denied", "pending", "adjusted", "voided"}

    def __init__(self, execution_date: datetime, config: Dict[str, Any] = None):
        self.execution_date = execution_date
        self.config = config or self._default_config()
        self.spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    def _default_config(self) -> Dict[str, Any]:
        return {
            "bronze_path":  "data/delta/bronze/claims",
            "silver_path":  "data/delta/silver/claims",
            "bad_records":  "data/delta/quarantine/claims",
        }

    # ─── PII Masking ──────────────────────────────────────────────────────────

    def mask_pii(self, df: DataFrame) -> DataFrame:
        """Hash PII fields using SHA-256 for HIPAA compliance."""
        for field in self.PII_FIELDS:
            if field in df.columns:
                df = df.withColumn(
                    field,
                    F.sha2(F.concat_ws("|", F.col(field), F.lit("SALT_2024")), 256)
                )
        return df

    # ─── Standardization ──────────────────────────────────────────────────────

    def standardize_codes(self, df: DataFrame) -> DataFrame:
        """Standardize medical codes and normalize formats."""
        return (
            df
            # ICD-10: uppercase, trim whitespace
            .withColumn("diagnosis_code",
                F.trim(F.upper(F.col("diagnosis_code"))))
            # CPT: zero-pad to 5 digits
            .withColumn("procedure_code",
                F.lpad(F.trim(F.col("procedure_code")), 5, "0"))
            # claim_type: lowercase
            .withColumn("claim_type",
                F.lower(F.trim(F.col("claim_type"))))
            # claim_status: lowercase
            .withColumn("claim_status",
                F.lower(F.trim(F.col("claim_status"))))
            # Standardize service_date to date only (no time)
            .withColumn("service_date",
                F.to_date(F.col("service_date")))
        )

    # ─── Validation ───────────────────────────────────────────────────────────

    def apply_validation_rules(self, df: DataFrame):
        """
        Apply business validation rules.
        Returns (valid_df, invalid_df).
        """
        valid_claim_types = list(self.VALID_CLAIM_TYPES)
        valid_statuses = list(self.VALID_STATUSES)

        validation_flags = (
            df
            .withColumn("_v_claim_id_null",
                F.col("claim_id").isNull())
            .withColumn("_v_member_id_null",
                F.col("member_id").isNull())
            .withColumn("_v_invalid_claim_type",
                ~F.col("claim_type").isin(valid_claim_types))
            .withColumn("_v_invalid_status",
                ~F.col("claim_status").isin(valid_statuses))
            .withColumn("_v_negative_amount",
                F.col("billed_amount") < 0)
            .withColumn("_v_future_date",
                F.col("service_date") > F.current_date())
            .withColumn("_v_paid_exceeds_billed",
                F.col("paid_amount") > F.col("billed_amount"))
        )

        flag_cols = [c for c in validation_flags.columns if c.startswith("_v_")]
        is_invalid = " OR ".join([f"`{c}` = true" for c in flag_cols])

        valid_df = (
            validation_flags
            .filter(~F.expr(is_invalid))
            .drop(*flag_cols)
        )

        invalid_df = (
            validation_flags
            .filter(F.expr(is_invalid))
            .withColumn("_validation_errors",
                F.concat_ws(", ", *[
                    F.when(F.col(c), F.lit(c.replace("_v_", ""))).otherwise(F.lit(None))
                    for c in flag_cols
                ]))
        )

        return valid_df, invalid_df

    # ─── Deduplication (CDC) ──────────────────────────────────────────────────

    def apply_cdc_dedup(self, df: DataFrame) -> DataFrame:
        """
        Apply Change Data Capture deduplication.
        Keep the most recent version of each claim_id.
        """
        window = (
            Window
            .partitionBy("claim_id")
            .orderBy(F.desc("_ingested_at"))
        )
        return (
            df
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    # ─── Enrichment ──────────────────────────────────────────────────────────

    def enrich(self, df: DataFrame) -> DataFrame:
        """Add derived/computed columns for analytics."""
        return (
            df
            .withColumn("claim_year",     F.year("service_date"))
            .withColumn("claim_month",    F.month("service_date"))
            .withColumn("claim_quarter",  F.quarter("service_date"))
            .withColumn("discount_rate",
                F.when(F.col("billed_amount") > 0,
                    F.round(1 - (F.col("allowed_amount") / F.col("billed_amount")), 4)
                ).otherwise(F.lit(None)))
            .withColumn("is_denied",
                (F.col("claim_status") == "denied").cast("boolean"))
            .withColumn("_silver_processed_at", F.current_timestamp())
        )

    # ─── Main Run ─────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        logger.info("Starting Bronze → Silver transformation")

        # 1. Read from Bronze
        df = (
            self.spark.read
            .format("delta")
            .load(self.config["bronze_path"])
            .filter(F.col("year") == self.execution_date.year)
            .filter(F.col("month") == self.execution_date.month)
        )
        input_count = df.count()
        logger.info(f"Loaded {input_count} records from Bronze")

        # 2. Mask PII
        df = self.mask_pii(df)

        # 3. Standardize codes
        df = self.standardize_codes(df)

        # 4. Validate
        valid_df, invalid_df = self.apply_validation_rules(df)
        invalid_count = invalid_df.count()

        if invalid_count > 0:
            logger.warning(f"Quarantining {invalid_count} invalid records")
            (
                invalid_df.write.format("delta")
                .mode("append")
                .save(self.config["bad_records"])
            )

        # 5. CDC Deduplication
        valid_df = self.apply_cdc_dedup(valid_df)

        # 6. Enrich
        valid_df = self.enrich(valid_df)

        # 7. Write Silver
        output_count = valid_df.count()
        (
            valid_df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere",
                f"claim_year = {self.execution_date.year} "
                f"AND claim_month = {self.execution_date.month}")
            .partitionBy("claim_year", "claim_month", "payer_id")
            .save(self.config["silver_path"])
        )

        logger.info(f"Silver write complete | output={output_count}")
        return {
            "input_records":    input_count,
            "output_records":   output_count,
            "quarantined":      invalid_count,
            "status":           "SUCCESS",
        }
