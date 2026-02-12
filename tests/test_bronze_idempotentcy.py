"""Unit tests for the Bronze layer idempotency logic."""

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from sirene_pipeline.services.bronze_job import run_ingestion_bronze


@pytest.fixture
def temp_db() -> duckdb.DuckDBPyConnection:
    """Provides a temporary in-memory DuckDB connection.

    Returns:
        A DuckDB connection object.
    """
    return duckdb.connect(database=":memory:")


def test_bronze_idempotency(tmp_path: Path, temp_db: duckdb.DuckDBPyConnection) -> None:
    """Checks if multiple ingestions of the same data result in no duplicates.

    This test ensures that running the ingestion twice with the same source
    data does not double the records in the registry or the output file.

    Args:
        tmp_path: Pytest fixture for temporary file management.
        temp_db: Pytest fixture for DuckDB connection.
    """
    # 1. Setup temporary paths
    fake_parquet_source: Path = tmp_path / "source_data.parquet"
    registry_db: Path = tmp_path / "bronze_metadata.db"
    output_parquet: Path = tmp_path / "bronze_output.parquet"
    dataset_name: str = "etablissements_test"

    # 2. Create a fake source Parquet file
    source_df: pd.DataFrame = pd.DataFrame(
        {"siret": ["11122233344455", "66677788899900"], "data": ["Company A", "Company B"]}
    )
    source_df.to_parquet(fake_parquet_source)

    # 3. First ingestion
    run_ingestion_bronze(
        url=str(fake_parquet_source),
        output_path=output_parquet,
        dataset_name=dataset_name,
        limit=0,
        registry_path=str(registry_db),
    )

    # 4. Verification of first run
    con: duckdb.DuckDBPyConnection = duckdb.connect(database=str(registry_db))
    res_tuple: tuple[Any, ...] | None = con.execute(
        f"SELECT COUNT(*) FROM {dataset_name}"
    ).fetchone()

    assert res_tuple is not None, "First run registry check failed to return results."
    res: Any = res_tuple[0]
    assert res == 2, "First run should ingest exactly 2 rows."
    con.close()

    # 5. Second run (Idempotence test)
    run_ingestion_bronze(
        url=str(fake_parquet_source),
        output_path=output_parquet,
        dataset_name=dataset_name,
        limit=0,
        registry_path=str(registry_db),
    )

    # 6. Final Verification
    con = duckdb.connect(database=str(registry_db))
    res_after_tuple: tuple[Any, ...] | None = con.execute(
        f"SELECT COUNT(*) FROM {dataset_name}"
    ).fetchone()

    assert res_after_tuple is not None, "Second run registry check failed."
    res_after: Any = res_after_tuple[0]
    assert res_after == 2, "Second run should not add duplicates."
    con.close()
