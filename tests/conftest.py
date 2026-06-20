"""Shared pytest fixtures and import-time environment for the orchestrator.

The partition definitions resolve their start dates from ``CORPUS_DATASETS_DIR``
at import time, so it must point at the fixture configs before any defs module
is imported. Setting it at conftest top level runs before test collection
imports those modules.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
DATASETS_DIR = FIXTURES / "datasets"
FAKE_CORPUS = Path(__file__).parent / "fake_corpus.py"

os.environ.setdefault("CORPUS_DATASETS_DIR", str(DATASETS_DIR))


@pytest.fixture
def corpus_binary(tmp_path: Path) -> str:
    """Writes a platform-appropriate launcher invoking the fake corpus script.

    The resource execs ``[binary_path, *args]`` directly (as in production), so
    the launcher wraps the Python interpreter around ``fake_corpus.py``.
    """
    if os.name == "nt":
        launcher = tmp_path / "corpus.cmd"
        launcher.write_text(
            f'@echo off\r\n"{sys.executable}" "{FAKE_CORPUS}" %*\r\n',
            encoding="utf-8",
        )
        return str(launcher)
    launcher = tmp_path / "corpus"
    launcher.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CORPUS}" "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    return str(launcher)


@pytest.fixture
def corpus(corpus_binary: str, tmp_path: Path):
    """A ``CorpusResource`` bound to the fake binary and a throwaway sink."""
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    sink = tmp_path / "sink"
    sink.mkdir()
    return CorpusResource(
        binary_path=corpus_binary,
        datasets_dir=str(DATASETS_DIR),
        sink_path=str(sink),
    )
