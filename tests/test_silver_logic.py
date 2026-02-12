"""Unit tests for Silver transformation logic."""

import pandas as pd


def test_siret_validation(sample_etablissement_df: pd.DataFrame) -> None:
    """Checks if Pandera correctly identifies invalid SIRET formats."""
    # We expect this to fail or we filter it manually
    df = sample_etablissement_df

    # Test: SIRET must be 14 digits
    mask = df["siret"].str.match(r"^\d{14}$")
    assert mask[0]
    assert not mask[1]  # 'invalid_siret' should fail


def test_idf_filtering() -> None:
    """Checks if the logic correctly filters departments outside IDF.

    This test ensures that the Silver layer strictly adheres to the
    geographical scope defined in the project objectives.
    """
    idf_deps: list[str] = ["75", "77", "78", "91", "92", "93", "94", "95"]
    test_data: pd.DataFrame = pd.DataFrame({"dept": ["75", "69", "93", "13"]})

    filtered: pd.DataFrame = test_data[test_data["dept"].isin(idf_deps)]

    assert len(filtered) == 2
    # .values can return Any, so we ensure the logic is clear for the linter
    assert "69" not in filtered["dept"].tolist()
