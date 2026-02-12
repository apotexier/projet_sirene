"""Data validation schemas for the Silver layer using Pandera."""

import pandera.pandas as pa
from pandera.typing import Series


class EtablissementSilverSchema(pa.DataFrameModel):
    """Schema for cleaned establishment data with strict validation."""

    # Identification
    siret: Series[str] = pa.Field(str_length=14, str_matches=r"^\d{14}$", unique=True)
    siren: Series[str] = pa.Field(str_length=9, str_matches=r"^\d{9}$")

    # Status & Dates
    etatAdministratifEtablissement: Series[str] = pa.Field(nullable=True)
    dateCreationEtablissement: Series[pa.DateTime] = pa.Field(nullable=True)
    # Geography
    codePostalEtablissement: Series[str] = pa.Field(
        str_length=5, str_matches=r"^\d{5}$", nullable=True
    )
    libelleCommuneEtablissement: Series[str] = pa.Field(nullable=True)

    # Activity & Workforce
    activitePrincipaleEtablissement: Series[str] = pa.Field(nullable=True)
    trancheEffectifsEtablissement: Series[str] = pa.Field(nullable=True)

    # Features
    etablissementSiege: Series[bool] = pa.Field(nullable=True)
    enseigne1Etablissement: Series[str] = pa.Field(nullable=True)

    # --- New Engineered Columns ---
    departement: Series[str] = pa.Field(nullable=True)
    secteur_activite: Series[str] = pa.Field(nullable=True)
    age_entreprise: Series[float] = pa.Field(nullable=True)

    # --- Incremental Metadata ---
    ingested_at: Series[pa.DateTime] = pa.Field(nullable=True)

    class Config:
        """Pandera configuration for the establishment schema."""

        coerce = True
        strict = True


class UniteLegaleSilverSchema(pa.DataFrameModel):
    """Schema for cleaned legal units data with strict validation."""

    # Identification
    siren: Series[str] = pa.Field(str_length=9, str_matches=r"^\d{9}$", unique=True)

    # Identity
    denominationUniteLegale: Series[str] = pa.Field(nullable=True)
    nomUniteLegale: Series[str] = pa.Field(nullable=True)
    prenom1UniteLegale: Series[str] = pa.Field(nullable=True)

    # Categories & Legal
    categorieEntreprise: Series[str] = pa.Field(nullable=True)
    categorieJuridiqueUniteLegale: Series[float] = pa.Field(nullable=True)

    # Activity & Status
    activitePrincipaleUniteLegale: Series[str] = pa.Field(nullable=True)
    etatAdministratifUniteLegale: Series[str] = pa.Field(nullable=True)
    dateCreationUniteLegale: Series[pa.DateTime] = pa.Field(nullable=True)

    # Social Economy
    economieSocialeSolidaireUniteLegale: Series[str] = pa.Field(nullable=True)

    # --- New Engineered Columns ---
    # Only activity_sector and company_age for Unites Legales (no postal code)
    secteur_activite: Series[str] = pa.Field(nullable=True)
    age_entreprise: Series[float] = pa.Field(nullable=True)

    # --- Incremental Metadata ---
    ingested_at: Series[pa.DateTime] = pa.Field(nullable=True)

    class Config:
        """Pandera configuration for the establishment schema."""

        coerce = True
        strict = True


SCHEMA_MAP = {
    "etablissements": EtablissementSilverSchema,
    "unites_legales": UniteLegaleSilverSchema,
}
