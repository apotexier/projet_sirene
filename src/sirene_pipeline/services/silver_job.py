"""Module for cleaning and validating SIRENE data from Bronze to Silver layer."""

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger
from pandera.errors import SchemaError

from sirene_pipeline.config import settings
from sirene_pipeline.utils.data_helpers import get_last_ingested_date
from sirene_pipeline.utils.silver_schemas import SCHEMA_MAP


def run_silver_transformation(dataset_name: str) -> None:
    """Performs incremental transformation, regional filtering, and cleaning.

    This function reads new Bronze data, applies business rules, handles 
    date boundary issues, cleans malformed postal codes, and validates schemas.

    Args:
        dataset_name: Name of the dataset to process ('etablissements' or 
            'unites_legales').

    Raises:
        ValueError: If dataset_name is missing from SCHEMA_MAP.
        KeyError: If configuration settings are missing.
        SchemaError: If final data fails Pandera validation.
    """
    logger.info(f"🚀 Starting Incremental Silver transformation for: {dataset_name}")

    if dataset_name not in SCHEMA_MAP:
        logger.error(f"❌ Dataset '{dataset_name}' is not defined in SCHEMA_MAP")
        raise ValueError(f"No schema defined for dataset: {dataset_name}")

    schema = SCHEMA_MAP[dataset_name]

    try:
        # Configuration setup
        bronze_file = settings.datasets[dataset_name].filename
        bronze_path = Path(settings.bronze_dir) / bronze_file
        silver_config = settings.silver.get(dataset_name)
        silver_dir = Path(settings.silver.output_dir)
        silver_dir.mkdir(parents=True, exist_ok=True)
        silver_output = silver_dir / f"{dataset_name}_silver.parquet"

        target_depts = settings.filters.get("idf_departments", [])
        selected_columns = silver_config.get("selected_columns")

        if not selected_columns:
            raise KeyError(f"No columns selected for {dataset_name} in settings.toml")

        if "ingested_at" not in selected_columns:
            selected_columns.append("ingested_at")

    except Exception as e:
        logger.error(f"❌ Configuration error for {dataset_name}: {e}")
        raise

    # 1. Incremental Check (Watermark)
    last_date = get_last_ingested_date(silver_output)
    logger.info(f"🔍 Checking for new data since: {last_date}")

    # 2. Extraction & Filtering via DuckDB
    con = duckdb.connect()
    try:
        where_clauses = [f"ingested_at > '{last_date}'"]
        if dataset_name == "etablissements" and target_depts:
            depts_str = ", ".join([f"'{d}'" for d in target_depts])
            where_clauses.append(f"substring(codePostalEtablissement, 1, 2) IN ({depts_str})")
        
        # Determine the date field to protect against Pandas 'Out of Bounds'
        date_creation_field = (
            "dateCreationEtablissement" if dataset_name == "etablissements" 
            else "dateCreationUniteLegale"
        )

        # Build SQL with date cleaning (Years < 1678 or > 2261 set to NULL)
        sql_columns = []
        for col in selected_columns:
            if col == date_creation_field:
                sql_columns.append(f"""
                    CASE 
                        WHEN {col} < '1678-01-01' THEN NULL 
                        WHEN {col} > '2261-12-31' THEN NULL
                        ELSE {col} 
                    END AS {col}
                """)
            else:
                sql_columns.append(col)
        
        query = f"""
            SELECT {", ".join(sql_columns)} 
            FROM read_parquet('{bronze_path}')
            WHERE {" AND ".join(where_clauses)}
        """
        new_df = con.execute(query).df()
        logger.info(f"📊 New rows extracted: {len(new_df):,}")

    except Exception as e:
        logger.error(f"❌ Extraction failed for {dataset_name}: {e}")
        raise
    finally:
        con.close()

    if new_df.empty:
        logger.success(f"✅ No new data to process for {dataset_name}.")
        return

    # 3. Data Cleaning & Feature Engineering
    logger.info("🧹 Cleaning and enriching new data")

    # Drop missing critical IDs
    id_cols = ["siret", "siren"] if "siret" in new_df.columns else ["siren"]
    new_df = new_df.dropna(subset=id_cols)

    # --- FIX: Robust Postal Code Cleaning (Fixes Pandera crashes) ---
    if "codePostalEtablissement" in new_df.columns:
        # Trim whitespace and filter for exact 5-digit format
        new_df["codePostalEtablissement"] = new_df["codePostalEtablissement"].astype(str).str.strip()
        valid_cp_mask = new_df["codePostalEtablissement"].str.match(r"^\d{5}$", na=False)
        invalid_count = (~valid_cp_mask).sum()
        
        if invalid_count > 0:
            logger.warning(f"🗑️ Dropped {invalid_count} malformed postal codes (noise in SIRENE)")
            new_df = new_df[valid_cp_mask].copy()

    # --- DATE CASTING & PRECISION ---
    date_cols = [c for c in new_df.columns if "date" in c.lower() or "ingested_at" in c]
    for col in date_cols:
        new_df[col] = pd.to_datetime(new_df[col], errors="coerce")
        if not new_df[col].isna().all():
            new_df[col] = new_df[col].dt.as_unit("ns")

    # Business rules: Default values
    fill_rules = {
        "etatAdministratifEtablissement": "A",
        "trancheEffectifsEtablissement": "00",
        "enseigne1Etablissement": "Non renseigné",
        "economieSocialeSolidaireUniteLegale": "N",
    }
    for col, value in fill_rules.items():
        if col in new_df.columns:
            new_df[col] = new_df[col].fillna(value)

    # --- FEATURE ENGINEERING ---
    if "codePostalEtablissement" in new_df.columns:
        new_df["departement"] = new_df["codePostalEtablissement"].str[:2]

    naf_col = "activitePrincipaleEtablissement" if "siret" in new_df.columns else "activitePrincipaleUniteLegale"
    if naf_col in new_df.columns:
        new_df["secteur_activite"] = new_df[naf_col].str[:2]

    if date_creation_field in new_df.columns:
        current_year = datetime.now().year
        new_df["age_entreprise"] = current_year - new_df[date_creation_field].dt.year
        new_df["age_entreprise"] = new_df["age_entreprise"].fillna(-1.0)

    # Final string fill
    string_cols = new_df.select_dtypes(include=["object"]).columns
    new_df[string_cols] = new_df[string_cols].fillna("Indéterminé")

    # 4. Merge with Existing Silver Data
    if silver_output.exists():
        logger.info("🔄 Merging with existing Silver storage")
        existing_df = pd.read_parquet(silver_output)
        final_df = pd.concat([existing_df, new_df])
        
        # Ensure overall data quality for postal codes after merge
        if "codePostalEtablissement" in final_df.columns:
            final_df = final_df[final_df["codePostalEtablissement"].str.match(r"^\d{5}$", na=False)]
            
        final_df = final_df.drop_duplicates(subset=id_cols, keep="last")
    else:
        final_df = new_df

    # 5. Data Quality Validation
    logger.info(f"🛡️ Validating combined {dataset_name} data")
    try:
        validated_df = schema.validate(final_df)
    except SchemaError as e:
        logger.error(f"🚨 Schema validation failed: {e}")
        raise

    # 6. Save to Silver Layer
    try:
        validated_df.to_parquet(silver_output, index=False, compression="snappy")
        logger.success(f"🏁 Transformation finished: {len(new_df)} new rows added.")
    except Exception as e:
        logger.error(f"❌ Failed to save Silver file: {e}")
        raise