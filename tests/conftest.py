"""Test isolation: redirect both the SQLite DB and the pair-registry CSV to
a throwaway tmp directory BEFORE any ``src.*`` modules import them.

Without this, tests would mutate the user's production DB and ah_pairs.csv —
soft-deleting real pairs during mock-based discovery tests, etc.

conftest.py is loaded by pytest before it collects test modules, which is
why mutating the environment here is effective.

The tmp CSV is seeded from the real ah_pairs.csv at session start so tests
that look up real pairs (e.g. test_mapping.py) pass. An autouse fixture
snapshots and restores the CSV around each test, so a single misbehaving
test cannot cascade into the next.
"""

import csv
import os
import pathlib
import shutil
import tempfile

import pytest

_TEST_DIR = pathlib.Path(tempfile.gettempdir()) / "ah-arb-pytest"
_TEST_DIR.mkdir(parents=True, exist_ok=True)

os.environ["AH_ARB_DB_DIR"] = str(_TEST_DIR)

# Wipe the session DB so each pytest invocation starts from scratch.
_db_file = _TEST_DIR / "data.db"
if _db_file.exists():
    _db_file.unlink()

# Seed tmp CSV from real CSV (or write header-only if missing).
_TEST_CSV = _TEST_DIR / "ah_pairs.csv"
_REAL_CSV = pathlib.Path(__file__).resolve().parent.parent / "ah_pairs.csv"
if _REAL_CSV.exists():
    shutil.copy(_REAL_CSV, _TEST_CSV)
else:
    with open(_TEST_CSV, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            "hk_code", "a_code", "name", "status",
            "is_red_chip", "source", "first_seen",
        ])

os.environ["AH_ARB_PAIRS_CSV"] = str(_TEST_CSV)


@pytest.fixture(autouse=True)
def _csv_snapshot_around_test():
    """Snapshot & restore the tmp CSV around each test so tests can't bleed
    state into one another (e.g. soft-delete wiping the seed)."""
    with open(_TEST_CSV, encoding="utf-8") as f:
        backup = f.read()
    yield
    with open(_TEST_CSV, "w", encoding="utf-8") as f:
        f.write(backup)
    # Lazy import — conftest is loaded before src.* and shouldn't import them.
    from src.data import ah_mapping
    ah_mapping.refresh_pairs_cache()
