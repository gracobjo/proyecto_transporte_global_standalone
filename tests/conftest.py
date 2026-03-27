from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


TESTS_DIR = Path(__file__).resolve().parent
DATA_DIR = TESTS_DIR / "data"
PROJECT_ROOT = TESTS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("SIMLOG_PlanPruebas")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def load_json_fixture():
    def _load(name: str):
        path = DATA_DIR / name
        return json.loads(path.read_text(encoding="utf-8"))

    return _load
