"""Module for incremental and idempotent ingestion of SIRENE data."""

import time
from datetime import datetime
from pathlib import Path

import duckdb
from loguru import logger
from tqdm import tqdm

# Assuming Dynaconf is used to load settings.toml
from sirene_pipeline.config import settings


def run_ingestion_bronze(
    url: str, output_path: Path,
    limit: int = 0, 
    dataset_name: str = "Dataset",
    registry_path: str = ""
) -> None:
    """Ingests SIRENE data incrementally using settings.toml.

    Args:
        url: URL of the source Parquet file.
        output_path: Local path for the output file.
        limit: Optional override for the row limit.
        dataset_name: Name of the dataset (used for table naming).
    """
    logger.info(f"{dataset_name}: Starting incremental Bronze ingestion")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. Database Connection using config path
    db_metadata_path = registry_path if len(registry_path)>0 else settings.bronze_registry
    con = duckdb.connect(database=db_metadata_path)

    # 2. Prepare Audit Metadata & Parameters
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Logic: 1. Use function arg 'limit' if provided
    #        2. Else use 'sample_limit' from settings.toml
    #        3. Default to 0 (Full load) if nothing found
    final_limit = limit if limit is not None else settings.get("sample_limit", 0)
    limit_clause = f"LIMIT {final_limit}" if final_limit > 0 else ""

    # Identify Primary Key (siren or siret)
    pk = "siret" if "etablissement" in dataset_name.lower() else "siren"

    # 3. SQL Logic: Anti-Join for Idempotency
    #
    query = f"""
        -- Create table structure if it doesn't exist
        CREATE TABLE IF NOT EXISTS {dataset_name} AS 
        SELECT *, '{ingested_at}' AS ingested_at FROM read_parquet('{url}') WHERE 1=0;

        -- Incremental Insert: Only rows where PK is not already in the table
        INSERT INTO {dataset_name}
        SELECT source.*, '{ingested_at}' as ingested_at
        FROM read_parquet('{url}') AS source
        LEFT JOIN {dataset_name} AS target ON source.{pk} = target.{pk}
        WHERE target.{pk} IS NULL
        {limit_clause};

        COPY {dataset_name} TO '{output_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
    """

    start_time = time.time()

    with tqdm(
        total=None,
        desc=f"Ingesting {dataset_name}",
        bar_format="{desc}: {elapsed} |{bar}| [Working...]",
        unit="s",
        leave=False,
    ) as pbar:
        try:
            con.execute(query)
            pbar.update(1)

            duration = time.time() - start_time
            result = con.execute(f"SELECT count(*) FROM {dataset_name}").fetchone()
            #handle case where result is None (e.g., if table is empty or query fails)
            row_count = result[0] if result else 0

            if row_count == 0:
                logger.warning(f"{dataset_name} No new rows ingested. Table is up to date.")

            logger.success(
                f"[{dataset_name}] Current total in registry: {row_count:,} rows ({duration:.2f}s)."
            )
        except Exception as e:
            logger.error(f"[{dataset_name}] Ingestion failed: {e}")
            raise
        finally:
            con.close()
