"""Main entry point for the SIRENE pipeline."""

from pathlib import Path
from loguru import logger
from sirene_pipeline.config import settings
from sirene_pipeline.services.bronze_job import run_ingestion_bronze

def main() -> None:
    """Orchestrate the ingestion of SIRENE datasets using Dynaconf settings."""
    logger.info(f"Launching Pipeline in environment: {settings.current_env}")

    # get the current environment (e.g., development, production) from Dynaconf settings
    env_name = settings.current_env
    logger.info(f"Running pipeline in {env_name} mode")

    # get the sample limit from settings, defaulting to 0 (full dataset) if not set
    limit = settings.get("sample_limit", 0)

    # Accessing datasets from config
    for name, config in settings.datasets.items():
        logger.info(f"Processing dataset: {name}")
        
        target_path = Path(settings.bronze_dir) / config.filename
        
        try:
            # load parquet files from the URL and save them to the Bronze layer
            run_ingestion_bronze(
                url=config.url, 
                output_path=target_path,
                limit=limit,
                dataset_name=name
            )
        except Exception as e:
            logger.error(f"Failed to process {name}: {e}")

if __name__ == "__main__":
    main()