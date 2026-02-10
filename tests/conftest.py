"""Shared test fixtures for the SIRENE pipeline."""

import pandas as pd
import pytest

@pytest.fixture
def sample_etablissement_df():
    """Returns a sample dataframe with mixed valid and invalid data."""
    return pd.DataFrame({
        "siret": ["12345678901234", "invalid_siret", "99999999999999"],
        "siren": ["123456789", "123456789", "999999999"],
        "codePostalEtablissement": ["75001", "69001", "95000"], # 69 is not IDF
        "dateCreationEtablissement": ["2020-01-01", "2020-01-01", "2025-12-31"],
        "etatAdministratifEtablissement": ["A", "A", "A"]
    })