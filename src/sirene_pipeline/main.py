"""Main entry point for the SIRENE pipeline orchestrating Bronze and Silver layers."""

from pathlib import Path

from loguru import logger

from sirene_pipeline.config import settings
from sirene_pipeline.services.bronze_job import run_ingestion_bronze
from sirene_pipeline.services.silver_job import run_silver_transformation


def main() -> None:
    """Orchestrate the ingestion and transformation of SIRENE datasets.

    This function handles the end-to-end pipeline:
    1. Bronze: Download and ingest raw Parquet files.
    2. Silver: Clean, filter, and validate data using Pandera.
    """
    logger.info(f"🚀 Launching Pipeline in environment: {settings.current_env}")

    # 1. Pipeline Configuration
    limit = settings.get("sample_limit", 0)
    datasets_to_process = list(settings.datasets.keys())

    # 2. BRONZE LAYER: Ingestion
    logger.info("--- Phase 1: Bronze Ingestion ---")
    successful_ingestions = []

    for name in datasets_to_process:
        config = settings.datasets[name]
        target_path = Path(settings.bronze_dir) / config.filename

        try:
            logger.info(f"Processing Bronze for: {name}")
            run_ingestion_bronze(
                url=config.url, output_path=target_path, limit=limit, dataset_name=name
            )
            successful_ingestions.append(name)
        except Exception as e:
            logger.error(f"❌ Bronze ingestion failed for {name}: {e}")

    # 3. SILVER LAYER: Transformation & Validation
    # We only process datasets that were successfully ingested in Bronze
    logger.info("--- Phase 2: Silver Transformation ---")

    for name in successful_ingestions:
        try:
            logger.info(f"Processing Silver for: {name}")
            run_silver_transformation(name)
        except Exception as e:
            logger.error(f"❌ Silver transformation failed for {name}: {e}")

    logger.success("✅ SIRENE Pipeline execution finished.")


if __name__ == "__main__":
    main()
