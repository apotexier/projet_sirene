"""Configuration management module using Dynaconf."""

from pathlib import Path

from dynaconf import Dynaconf

# Determine the base path of the project
current_dir = Path(__file__).parent

settings = Dynaconf(
    envvar_prefix="SIRENE",
    settings_files=[current_dir / "settings.toml"],
    environments=True,
    load_dotenv=True,
)
