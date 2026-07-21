"""Test Configuration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from singer_sdk.testing.templates import TargetFileTestTemplate

if TYPE_CHECKING:
    import pytest


pytest_plugins = ()


class TargetClickhouseFileTestTemplate(TargetFileTestTemplate):
    """Base Target File Test Template.

    Use this when sourcing Target test input from a .singer file.
    """

    @property
    def singer_filepath(self):
        """Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns:
            The expected Path to this tests singer file.

        """
        current_file_path = Path(__file__).resolve()
        return current_file_path.parent / "target_test_streams" / f"{self.name}.singer"


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest."""
    if sys.version_info < (3, 11):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.10 will reach its end of life on 2026-10:FutureWarning",
        )
