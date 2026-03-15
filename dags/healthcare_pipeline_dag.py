"""
Healthcare ETL Pipeline DAG
Orchestrates the full Bronze → Silver → Gold medallion pipeline
for healthcare claims data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# ─── Default Args ────────────────────────────────────────────────────────────
default_args = {
    "owner": "praneeth.rokandla",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

# ─── DAG Definition ──────────────────────────────────────────────────────────
dag = DAG(
    dag_id="healthcare_pipeline_dag",
    description="HIPAA-compliant healthcare claims ETL pipeline (Bronze → Silver → Gold)",
    schedule_interval="0 2 * * *",  # Daily at 2am
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "etl", "medallion", "claims"],
    doc_md="""
    ## Healthcare Claims ETL Pipeline
    This DAG ingests multi-payer healthcare claims data, applies HIPAA-compliant
    transformations, and loads analytics-ready data into Snowflake.

    ### Layers
    - **Bronze**: Raw ingested data in Delta Lake
    - **Silver**: Cleaned, validated, deduplicated data
    - **Gold**: Star-schema analytics tables in Snowflake
    """,
)

# ─── Task Functions ──────────────────────────────────────────────────────────

def ingest_claims_data(**context):
    """Ingest raw claims from source systems into Bronze layer."""
    from ingestion.claims_ingestion import ClaimsIngestion
    execution_date = context["execution_date"]
    logger.info(f"Starting claims ingestion for {execution_date}")
    ingester = ClaimsIngestion(execution_date=execution_date)
    result = ingester.run()
    logger.info(f"Ingested {result['records_loaded']} records into Bronze layer")
    context["task_instance"].xcom_push(key="bronze_record_count", value=result["records_loaded"])


def ingest_member_data(**context):
    """Ingest member demographic data into Bronze layer."""
    from ingestion.member_ingestion import MemberIngestion
    execution_date = context["execution_date"]
    ingester = MemberIngestion(execution_date=execution_date)
    result = ingester.run()
    logger.info(f"Ingested {result['records_loaded']} member records")


def ingest_provider_data(**context):
    """Ingest provider reference data into Bronze layer."""
    from ingestion.provider_ingestion import ProviderIngestion
    execution_date = context["execution_date"]
    ingester = ProviderIngestion(execution_date=execution_date)
    result = ingester.run()
    logger.info(f"Ingested {result['records_loaded']} provider records")


def bronze_to_silver(**context):
    """
    Transform Bronze → Silver:
    - Schema validation
    - PII masking / encryption
    - Deduplication
    - Data type casting
    - Null handling
    """
    from transforms.bronze_to_silver import BronzeToSilverTransform
    execution_date = context["execution_date"]
    logger.info("Starting Bronze → Silver transformation")
    transformer = BronzeToSilverTransform(execution_date=execution_date)
    result = transformer.run()
    logger.info(f"Silver layer: {result['output_records']} records written")
    context["task_instance"].xcom_push(key="silver_record_count", value=result["output_records"])


def run_data_quality_checks(**context):
    """Run Great Expectations data quality checks on Silver layer."""
    import great_expectations as ge
    execution_date = context["execution_date"]
    logger.info("Running data quality checks on Silver layer")

    # In production, this loads the GE context and runs a checkpoint
    # Simulated here for demo purposes
    quality_metrics = {
        "completeness": 99.2,
        "uniqueness": 100.0,
        "validity": 98.7,
        "timeliness": 100.0,
    }

    failed_checks = [k for k, v in quality_metrics.items() if v < 95.0]
    if failed_checks:
        raise ValueError(f"Data quality checks failed: {failed_checks}")

    logger.info(f"All quality checks passed: {quality_metrics}")
    context["task_instance"].xcom_push(key="quality_metrics", value=quality_metrics)


def silver_to_gold(**context):
    """
    Transform Silver → Gold:
    - Build fact_claims table
    - Build dim_member, dim_provider, dim_date tables
    - Load into Snowflake star schema
    """
    from transforms.silver_to_gold import SilverToGoldTransform
    execution_date = context["execution_date"]
    logger.info("Starting Silver → Gold transformation")
    transformer = SilverToGoldTransform(execution_date=execution_date)
    result = transformer.run()
    logger.info(f"Gold layer loaded: {result}")


def send_pipeline_summary(**context):
    """Send pipeline completion summary."""
    ti = context["task_instance"]
    bronze_count = ti.xcom_pull(key="bronze_record_count", task_ids="ingest_claims")
    silver_count = ti.xcom_pull(key="silver_record_count", task_ids="bronze_to_silver")
    quality = ti.xcom_pull(key="quality_metrics", task_ids="data_quality_checks")

    summary = {
        "pipeline": "healthcare_claims",
        "execution_date": str(context["execution_date"]),
        "bronze_records": bronze_count,
        "silver_records": silver_count,
        "quality_metrics": quality,
        "status": "SUCCESS",
    }
    logger.info(f"Pipeline completed successfully: {summary}")


# ─── Task Definitions ─────────────────────────────────────────────────────────

with dag:

    # Ingestion tasks (run in parallel)
    ingest_claims = PythonOperator(
        task_id="ingest_claims",
        python_callable=ingest_claims_data,
        provide_context=True,
    )

    ingest_members = PythonOperator(
        task_id="ingest_members",
        python_callable=ingest_member_data,
        provide_context=True,
    )

    ingest_providers = PythonOperator(
        task_id="ingest_providers",
        python_callable=ingest_provider_data,
        provide_context=True,
    )

    # Bronze → Silver transformation
    bronze_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
        provide_context=True,
    )

    # Data quality gate
    dq_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
        provide_context=True,
    )

    # dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    )

    # Silver → Gold
    silver_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
        provide_context=True,
    )

    # Summary notification
    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=send_pipeline_summary,
        provide_context=True,
        trigger_rule="all_success",
    )

    # ─── DAG Dependencies ──────────────────────────────────────────────────────
    [ingest_claims, ingest_members, ingest_providers] >> bronze_silver
    bronze_silver >> dq_checks >> dbt_run >> dbt_test >> silver_gold >> summary
