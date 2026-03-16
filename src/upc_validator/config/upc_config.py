from __future__ import annotations

DATASET_NAME = "UPC"

EXPECTED_COLUMNS_BASELINE = [
    "UPC_Code",
    "Item_Number",
    "Timestamp_run",
]

EXPECTED_BASELINE_COLUMN_COUNT = 3

KEY_UPC_COLUMN = "UPC_Code"
KEY_ITEM_COLUMN = "Item_Number"
KEY_TIMESTAMP_COLUMN = "Timestamp_run"

SNAPSHOT_CANDIDATE_COLUMNS = ["Timestamp_run"]

MAX_EVIDENCE_ROWS = 1000

STRICT_SCHEMA_MODE = True
STRICT_SNAPSHOT_MODE = False
STRICT_UNIVERSE_MODE = True
FAIL_ON_EMPTY_FILES = True

INVALID_TEXT_VALUES = {"null", "none", "nan", "nat"}