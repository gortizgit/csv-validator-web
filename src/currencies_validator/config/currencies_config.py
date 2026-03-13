from __future__ import annotations

DATASET_NAME = "Currencies"

EXPECTED_COLUMNS_BASELINE = [
    "Country Code",
    "date",
    "Warehouse Number",
    "Currency",
    "divider",
    "Exchange Rate",
]

EXPECTED_BASELINE_COLUMN_COUNT = 6

KEY_COUNTRY_COLUMN = "Country Code"
KEY_DATE_COLUMN = "date"
KEY_WAREHOUSE_COLUMN = "Warehouse Number"
KEY_CURRENCY_COLUMN = "Currency"

SNAPSHOT_CANDIDATE_COLUMNS = []

MAX_EVIDENCE_ROWS = 500

STRICT_SCHEMA_MODE = True
STRICT_SNAPSHOT_MODE = True
STRICT_UNIVERSE_MODE = True