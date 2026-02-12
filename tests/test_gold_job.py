"""Unit tests for the Gold layer logic (Enrichment and KPIs)."""

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from sirene_pipeline.config import settings
from sirene_pipeline.services.gold_job import run_gold_layer


def test_gold_logic_and_kpis(tmp_path: Path) -> None:
    """Verifies that the Master table is correctly joined and KPIs are accurate.

    Args:
        tmp_path: Pytest fixture providing a temporary directory for test files.
    """
    # 1. Setup paths
    silver_dir: Path = tmp_path / "silver"
    gold_dir: Path = tmp_path / "gold"
    silver_dir.mkdir()
    gold_dir.mkdir()

    # 2. Create mock Silver data
    etabs_df: pd.DataFrame = pd.DataFrame(
        {
            "siret": ["111001", "111002", "222001"],
            "siren": ["111", "111", "222"],
            "departement": ["75", "75", "92"],
            "secteur_activite": ["IT", "IT", "Bakery"],
        }
    )

    ul_df: pd.DataFrame = pd.DataFrame(
        {
            "siren": ["111", "222"],
            "denominationUniteLegale": ["TechCorp", "BreadInc"],
            "nomUniteLegale": [None, None],
            "prenom1UniteLegale": [None, None],
            "categorieEntreprise": ["PME", "GE"],
            "categorieJuridiqueUniteLegale": ["5710", "5499"],
            "economieSocialeSolidaireUniteLegale": ["N", "N"],
        }
    )

    etabs_df.to_parquet(silver_dir / "etablissements_silver.parquet")
    ul_df.to_parquet(silver_dir / "unites_legales_silver.parquet")

    # 3. Run the Gold Layer
    run_gold_layer(custom_silver_dir=silver_dir, custom_gold_dir=gold_dir)

    # 4. VERIFICATIONS
    con: duckdb.DuckDBPyConnection = duckdb.connect()

    # Check A: Master Table
    master_path: Path = gold_dir / settings.gold.master_filename
    assert master_path.exists()

    # Check B: KPI 1 (Departments)
    kpi_dept_path: Path = gold_dir / settings.gold.kpis.dept_dist
    assert kpi_dept_path.exists(), f"Missing KPI file: {kpi_dept_path}"

    # Fetch and check result for Dept 75
    res_dept: tuple[Any, ...] | None = con.execute(f"""
        SELECT total_establishments 
        FROM read_parquet('{kpi_dept_path.as_posix()}') 
        WHERE departement='75'
    """).fetchone()

    assert res_dept is not None, "KPI 1 query returned no results"
    assert res_dept[0] == 2

    # Check C: KPI 3 (Company Size)
    kpi_size_path: Path = gold_dir / settings.gold.kpis.size_dist
    assert kpi_size_path.exists()

    res_size: tuple[Any, ...] | None = con.execute(f"""
        SELECT total 
        FROM read_parquet('{kpi_size_path.as_posix()}') 
        WHERE categorieEntreprise='PME'
    """).fetchone()

    assert res_size is not None, "KPI 3 query returned no results"
    assert res_size[0] == 2

    con.close()
