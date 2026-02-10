"""Module for the Gold layer: Data enrichment and KPI generation.

This module joins Silver datasets to create a unified 'Master' table and 
computes key performance indicators (KPIs) for business analysis.
"""

from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from sirene_pipeline.config import settings

@monitor_step
def run_gold_layer(
    custom_silver_dir: Optional[Path] = None, 
    custom_gold_dir: Optional[Path] = None
) -> None:
    """Processes the Gold layer by enriching data and calculating KPIs.

    The process follows two main phases:
    1. Denormalization: Left join between Establishments and Legal Units to 
       create a comprehensive master file.
    2. Aggregation: Computation of three specific KPIs defined in requirements.

    Args:
        custom_silver_dir: Optional override for the input Silver directory (used for tests).
        custom_gold_dir: Optional override for the output Gold directory (used for tests).

    Raises:
        FileNotFoundError: If required Silver files are missing.
        Exception: For any errors during SQL execution or file writing.
    """
    logger.info("🏆 Starting Gold layer processing")

    # 1. Path Configuration
    # We resolve the directories: priority to custom paths, otherwise use settings
    silver_dir: Path = custom_silver_dir or Path(settings.silver.output_dir)
    gold_dir: Path = custom_gold_dir or Path(settings.gold.output_dir)
    
    gold_dir.mkdir(parents=True, exist_ok=True)

    # Use .as_posix() for SQL compatibility across all Operating Systems
    etab_path: str = (silver_dir / "etablissements_silver.parquet").as_posix()
    ul_path: str = (silver_dir / "unites_legales_silver.parquet").as_posix()
    master_path: str = (gold_dir / settings.gold.master_filename).as_posix()

    # Verify input availability
    if not Path(etab_path).exists() or not Path(ul_path).exists():
        logger.error(f"❌ Silver files missing in {silver_dir}. Run Silver job first.")
        raise FileNotFoundError("Missing required Silver parquet files.")

    con = duckdb.connect(database=":memory:")

    try:
        # STEP 1: ENRICHMENT (Denormalization)
        # ---------------------------------------------------------
        logger.info(f"🔗 Creating enriched Master Table: {settings.gold.master_filename}")
        con.execute(f"""
            COPY (
                SELECT 
                    e.*,
                    ul.denominationUniteLegale,
                    ul.nomUniteLegale,
                    ul.prenom1UniteLegale,
                    ul.categorieEntreprise,
                    ul.categorieJuridiqueUniteLegale,
                    ul.economieSocialeSolidaireUniteLegale
                FROM read_parquet('{etab_path}') AS e
                LEFT JOIN read_parquet('{ul_path}') AS ul ON e.siren = ul.siren
            ) TO '{master_path}' (FORMAT PARQUET);
        """)

        # STEP 2: KPI GENERATION (Aggregations)
        # ---------------------------------------------------------
        
        # KPI 1: Establishments by Department
        logger.info(f"📊 Calculating: {settings.gold.kpis.dept_dist}")
        con.execute(f"""
            COPY (
                SELECT 
                    departement, 
                    COUNT(*) as total_establishments
                FROM read_parquet('{master_path}')
                GROUP BY departement
                ORDER BY total_establishments DESC
            ) TO '{(gold_dir / settings.gold.kpis.dept_dist).as_posix()}' (FORMAT PARQUET);
        """)

        # KPI 2: Dominant Sectors per Department
        logger.info(f"📊 Calculating: {settings.gold.kpis.sectors}")
        con.execute(f"""
            COPY (
                WITH secteur_activite AS (
                    SELECT 
                        departement, 
                        secteur_activite, 
                        COUNT(*) as count
                    FROM read_parquet('{master_path}')
                    GROUP BY ALL
                )
                SELECT departement, secteur_activite, count
                FROM secteur_activite
                QUALIFY ROW_NUMBER() OVER(PARTITION BY departement ORDER BY count DESC) = 1
            ) TO '{(gold_dir / settings.gold.kpis.sectors).as_posix()}' (FORMAT PARQUET);
        """)

        # KPI 3: Company Size Distribution
        logger.info(f"📊 Calculating: {settings.gold.kpis.size_dist}")
        con.execute(f"""
            COPY (
                SELECT 
                    categorieEntreprise, 
                    COUNT(*) as total
                FROM read_parquet('{master_path}')
                WHERE categorieEntreprise IS NOT NULL
                GROUP BY categorieEntreprise
                ORDER BY total DESC
            ) TO '{(gold_dir / settings.gold.kpis.size_dist).as_posix()}' (FORMAT PARQUET);
        """)

        logger.success(f"🏁 Gold layer finished. Files saved in: {gold_dir}")

    except Exception as e:
        logger.error(f"❌ Gold layer failed: {e}")
        raise
    finally:
        con.close()