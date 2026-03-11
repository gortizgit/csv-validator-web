from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


@dataclass
class CheckResult:
    check_id: str
    check_name: str
    status: str
    category: str
    dataset: str = "Prices"
    country: str = "ALL"
    field: str = ""
    expected: str = ""
    actual: str = ""
    details: str = ""
    evidence_file: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationRun:
    summary: Dict[str, Any]
    checks: List[CheckResult]
    dataframes: Optional[Dict[str, Any]] = None


@dataclass
class SnapshotContext:
    domo_snapshot: str = ""
    informatica_snapshot: str = ""
