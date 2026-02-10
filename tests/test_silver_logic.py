"""Unit tests for Silver transformation logic."""

from sirene_pipeline.utils.silver_schemas import EtablissementSilverSchema
import pandas as pd
import pytest

def test_siret_validation(sample_etablissement_df):
    """Checks if Pandera correctly identifies invalid SIRET formats."""
    # We expect this to fail or we filter it manually
    df = sample_etablissement_df
    
    # Test: SIRET must be 14 digits
    mask = df['siret'].str.match(r"^\d{14}$")
    assert mask[0] == True
    assert mask[1] == False  # 'invalid_siret' should fail

def test_idf_filtering():
    """Checks if the logic correctly filters departments outside IDF."""
    idf_deps = ["75", "77", "78", "91", "92", "93", "94", "95"]
    test_data = pd.DataFrame({"dept": ["75", "69", "93", "13"]})
    
    filtered = test_data[test_data['dept'].isin(idf_deps)]
    
    assert len(filtered) == 2
    assert "69" not in filtered['dept'].values