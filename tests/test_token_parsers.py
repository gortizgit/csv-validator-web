import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from prices_validator.core.token_parsers import (
    extract_store_sequence_from_cost_center,
    extract_store_sequence_from_structured,
    parse_country_store_metric,
)


def test_cost_center_parser():
    assert extract_store_sequence_from_cost_center("CR001|CR002") == ["CR001", "CR002"]


def test_price_parser_sequence():
    raw = "CR~001:12.34|CR~002:15.99"
    stores = extract_store_sequence_from_structured(raw, expected_country="CR", kind="price")
    assert stores == ["001", "002"]


def test_date_parser_errors():
    parsed = parse_country_store_metric("XX001:2025-10-10", expected_country="CR", kind="date")
    assert parsed["errors"]
