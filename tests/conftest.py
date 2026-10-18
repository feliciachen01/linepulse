import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

# Tests default to the parquet engine so they run anywhere (no Maven download).
# Set LINEPULSE_ENGINE=delta to run the same tests against Delta Lake.
ENGINE = os.environ.get("LINEPULSE_ENGINE", "parquet")


@pytest.fixture(scope="session")
def spark():
    from run_local import local_spark
    s = local_spark(ENGINE)
    yield s
    s.stop()


@pytest.fixture
def engine():
    return ENGINE
