"""Automated quality assurance script for Ruff and Mypy checks."""

import subprocess
import sys

from loguru import logger


def run_command(command: list[str], description: str) -> bool:
    """Executes a shell command and returns True if successful.

    Args:
        command: List of command arguments.
        description: Friendly name of the check.

    Returns:
        Boolean indicating success or failure.
    """
    logger.info(f"🚀 Running {description}...")
    try:
        # We run the command and pipe output to terminal
        process = subprocess.run(command, check=False)
        if process.returncode == 0:
            logger.success(f"✅ {description} passed.")
            return True
        else:
            logger.error(f"❌ {description} failed with exit code {process.returncode}.")
            return False
    except FileNotFoundError:
        logger.error(f"❌ Tool not found. Make sure {command[0]} is installed.")
        return False


def main() -> None:
    """Runs the full QA suite: Formatting, Linting, and Type Checking."""
    logger.info("🧪 Starting Quality Assurance suite...")

    # Define our pipeline of checks
    qa_pipeline = [
        (["ruff", "format", "."], "Ruff Formatter"),
        (["ruff", "check", ".", "--fix"], "Ruff Linter (with auto-fix)"),
        (["mypy", "."], "Mypy Type Checking"),
    ]

    all_passed = True
    for cmd, desc in qa_pipeline:
        if not run_command(cmd, desc):
            all_passed = False
            # We don't stop immediately so we can see all errors at once

    if all_passed:
        logger.success("✨ Everything is perfect! Your code is industrial-grade.")
    else:
        logger.warning("⚠️ Some checks failed. Please fix the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
