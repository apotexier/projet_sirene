"""Unit tests for the Bronze layer idempotency logic."""

import duckdb
import pytest
import pandas as pd
from pathlib import Path
from sirene_pipeline.services.bronze_job import run_ingestion_bronze

@pytest.fixture
def temp_db():
    """Provides a temporary in-memory DuckDB connection."""
    return duckdb.connect(database=":memory:")

def test_bronze_idempotency(tmp_path, temp_db):
    """Checks if multiple ingestions of the same data result in no duplicates.
    
    This test ensures that running the ingestion twice with the same source
    data does not double the records in the registry or the output file.
    """
    
    # 1. Setup temporary paths (Separating Registry from Output Parquet)
    fake_parquet_source = tmp_path / "source_data.parquet"
    registry_db = tmp_path / "bronze_metadata.db" 
    output_parquet = tmp_path / "bronze_output.parquet"
    dataset_name = "etablissements_test"

    # 2. Create a fake source Parquet file
    source_df = pd.DataFrame({
        "siret": ["11122233344455", "66677788899900"],
        "data": ["Company A", "Company B"]
    })
    source_df.to_parquet(fake_parquet_source)

    # 3. First ingestion
    # We explicitly pass registry_path to isolate the test from production settings
    run_ingestion_bronze(
        url=str(fake_parquet_source),
        output_path=output_parquet,
        dataset_name=dataset_name,
        limit=0,
        registry_path=str(registry_db)
    )

    # 4. Verification of first run
    con = duckdb.connect(database=str(registry_db))
    res = con.execute(f"SELECT COUNT(*) FROM {dataset_name}").fetchone()[0]
    assert res == 2, "First run should ingest exactly 2 rows."
    con.close()

    # 5. Second run (Idempotence test)
    # Using the exact same source and registry
    run_ingestion_bronze(
        url=str(fake_parquet_source),
        output_path=output_parquet,
        dataset_name=dataset_name,
        limit=0,
        registry_path=str(registry_db)
    )

    # 6. Final Verification
    con = duckdb.connect(database=str(registry_db))
    res_after = con.execute(f"SELECT COUNT(*) FROM {dataset_name}").fetchone()[0]
    assert res_after == 2, "Second run should not add duplicates. Total count must remain 2."
    con.close()