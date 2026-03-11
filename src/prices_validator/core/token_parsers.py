from __future__ import annotations

import re
from typing import Dict, List, Tuple

BLANK_LIKE_VALUES = {"", "NULL", "null", "None", "none", "NaN", "nan", "<NA>"}
PRICE_PATTERN = re.compile(r"^(?P<country>[A-Z]{2})~(?P<store>[^:]+):(?P<price>-?\d+(?:\.\d+)?)$")
DATE_PATTERN = re.compile(r"^(?P<country>[A-Z]{2})~(?P<store>[^:]+):(?P<date>\d{4}-\d{2}-\d{2})$")
CODE_PATTERN = re.compile(r"^[A-Z]{2,3}$")


def is_blank_like(value: str) -> bool:
    return value in BLANK_LIKE_VALUES


def split_pipe_tokens(raw: str) -> List[str]:
    if is_blank_like(raw):
        return []
    return raw.split("|")


def parse_cost_center(raw: str) -> Dict[str, object]:
    tokens = split_pipe_tokens(raw)
    return {
        "raw": raw,
        "tokens": tokens,
        "token_count": len(tokens),
        "blank": is_blank_like(raw),
        "errors": [],
    }


def parse_country_store_metric(raw: str, expected_country: str, kind: str) -> Dict[str, object]:
    if is_blank_like(raw):
        return {"raw": raw, "entries": [], "token_count": 0, "blank": True, "errors": []}

    pattern = PRICE_PATTERN if kind == "price" else DATE_PATTERN
    entries = []
    errors = []

    for token in raw.split("|"):
        match = pattern.match(token)
        if not match:
            errors.append(f"Invalid token format: {token}")
            continue
        token_country = match.group("country")
        if token_country != expected_country:
            errors.append(f"Country mismatch in token '{token}' (expected {expected_country})")
        payload_key = "price" if kind == "price" else "date"
        entries.append(
            {
                "country": token_country,
                "store": match.group("store"),
                payload_key: match.group(payload_key),
                "raw": token,
            }
        )

    return {
        "raw": raw,
        "entries": entries,
        "token_count": len(raw.split("|")),
        "blank": False,
        "errors": errors,
    }


def validate_code_value(raw: str, expected_country: str | None = None) -> Tuple[bool, str]:
    if is_blank_like(raw):
        return True, "blank-preserved"
    if not CODE_PATTERN.match(raw):
        return False, f"Invalid code format: {raw}"
    if expected_country and raw != expected_country:
        return False, f"Expected {expected_country}, got {raw}"
    return True, "ok"


def extract_store_sequence_from_cost_center(raw: str) -> List[str]:
    return split_pipe_tokens(raw)


def extract_store_sequence_from_structured(raw: str, expected_country: str, kind: str) -> List[str]:
    parsed = parse_country_store_metric(raw, expected_country=expected_country, kind=kind)
    return [entry["store"] for entry in parsed["entries"]]
