"""Shared project configuration loading."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent


def load_project_env() -> None:
    """Load the repository-root .env without overriding shell variables."""
    load_dotenv(PROJECT_ROOT / ".env", override=False)
