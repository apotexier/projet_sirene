"""Main entry point for the SIRENE pipeline orchestrating Bronze, Silver, and Gold layers."""

import time
from pathlib import Path

from loguru import logger

from sirene_pipeline.config import settings
from sirene_pipeline.services.bronze_job import run_ingestion_bronze
from sirene_pipeline.services.gold_job import run_gold_layer
from sirene_pipeline.services.silver_job import run_silver_transformation


def main() -> None:
    """Orchestrates the end-to-end SIRENE data pipeline.

    The execution flow is:
    1. Bronze: Incremental ingestion of raw Parquet files into a local registry.
    2. Silver: Cleaning, filtering (IDF), and validation using Pandera.
    3. Gold: Data enrichment (join) and business KPI generation.
    """
    start_time = time.time()
    logger.info(f"🚀 Launching Pipeline in environment: {settings.current_env}")

    # 1. Pipeline Configuration
    limit = settings.get("sample_limit", 0)
    datasets_to_process = list(settings.datasets.keys())

    # 2. BRONZE LAYER: Ingestion
    # We collect successful ingestions to know what can be processed in Silver
    logger.info("--- Phase 1: Bronze Ingestion ---")
    successful_bronze = []

    for name in datasets_to_process:
        config = settings.datasets[name]
        target_path = Path(settings.bronze_dir) / config.filename

        try:
            logger.info(f"Processing Bronze for: {name}")
            run_ingestion_bronze(
                url=config.url, output_path=target_path, limit=limit, dataset_name=name
            )
            successful_bronze.append(name)
        except Exception as e:
            logger.error(f"❌ Bronze ingestion failed for {name}: {e}")

    # 3. SILVER LAYER: Transformation & Validation
    # We collect successful transformations to decide if Gold can run
    logger.info("--- Phase 2: Silver Transformation ---")
    successful_silver = []

    for name in successful_bronze:
        try:
            logger.info(f"Processing Silver for: {name}")
            run_silver_transformation(name)
            successful_silver.append(name)
        except Exception as e:
            logger.error(f"❌ Silver transformation failed for {name}: {e}")

    # 4. GOLD LAYER: Enrichment & KPIs
    # Logic: Gold requires BOTH datasets (etablissements AND unites_legales)
    # because it performs a join between them to create the master table.
    logger.info("--- Phase 3: Gold Aggregation ---")

    if len(successful_silver) == len(datasets_to_process):
        try:
            logger.info("Processing Gold layer (Master table + KPIs)")
            run_gold_layer()
            logger.success("🏆 Gold layer completed successfully.")
        except Exception as e:
            logger.error(f"❌ Gold layer failed: {e}")
    else:
        logger.warning(
            "⚠️ Gold layer skipped. All Silver datasets must be successful to perform joins. "
            f"Successful: {successful_silver}"
        )

    total_duration = time.time() - start_time
    logger.success(f"✅ SIRENE Pipeline execution finished in {total_duration:.2f}s.")


if __name__ == "__main__":
    main()
