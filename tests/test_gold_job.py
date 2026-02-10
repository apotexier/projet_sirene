"""Unit tests for the Gold layer logic (Enrichment and KPIs)."""

import pandas as pd
import duckdb
import pytest
from pathlib import Path
from sirene_pipeline.services.gold_job import run_gold_layer
from sirene_pipeline.config import settings

def test_gold_logic_and_kpis(tmp_path):
    """Verifies that the Master table is correctly joined and KPIs are accurate."""
    
    # 1. Setup paths
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir()
    gold_dir.mkdir()

    # 2. Create mock Silver data
    # We create 3 establishments, 2 of which belong to the same company
    etabs_df = pd.DataFrame({
        "siret": ["111001", "111002", "222001"],
        "siren": ["111", "111", "222"],
        "departement": ["75", "75", "92"],
        "secteur_activite": ["IT", "IT", "Bakery"]
    })
    
    # We create 2 Legal Units
    ul_df = pd.DataFrame({
    "siren": ["111", "222"],
    "denominationUniteLegale": ["TechCorp", "BreadInc"],
    "nomUniteLegale": [None, None],        # Ajouté
    "prenom1UniteLegale": [None, None],    # Ajouté
    "categorieEntreprise": ["PME", "GE"],
    "categorieJuridiqueUniteLegale": ["5710", "5499"], # Ajouté
    "economieSocialeSolidaireUniteLegale": ["N", "N"]  # Ajouté
    })

    etabs_df.to_parquet(silver_dir / "etablissements_silver.parquet")
    ul_df.to_parquet(silver_dir / "unites_legales_silver.parquet")

    # 3. Run the Gold Layer
    run_gold_layer(custom_silver_dir=silver_dir, custom_gold_dir=gold_dir)

    # 4. VERIFICATIONS
    con = duckdb.connect()
    
    # Check A: Master Table
    master_path = gold_dir / settings.gold.master_filename
    assert master_path.exists()
    
    # Check B: KPI 1 (Departments)
    # On utilise le nom dynamique défini dans le TOML
    kpi_dept_path = gold_dir / settings.gold.kpis.dept_dist
    assert kpi_dept_path.exists(), f"Missing KPI file: {kpi_dept_path}"
    
    kpi_dept = con.execute(f"""
        SELECT total_establishments 
        FROM read_parquet('{kpi_dept_path.as_posix()}') 
        WHERE departement='75'
    """).fetchone()[0]
    assert kpi_dept == 2

    # Check C: KPI 3 (Company Size)
    kpi_size_path = gold_dir / settings.gold.kpis.size_dist
    assert kpi_size_path.exists()
    
    kpi_size = con.execute(f"""
        SELECT total 
        FROM read_parquet('{kpi_size_path.as_posix()}') 
        WHERE categorieEntreprise='PME'
    """).fetchone()[0]
    assert kpi_size == 2

    con.close()