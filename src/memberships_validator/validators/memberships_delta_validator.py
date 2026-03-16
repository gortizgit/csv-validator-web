from __future__ import annotations

from collections import Counter
from typing import Dict, List, Tuple

import pandas as pd

from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun
from memberships_validator.config.memberships_delta_config import (
    BOOLEAN_LIKE_COLUMNS,
    COUNTRY_COLUMNS,
    DATASET_NAME,
    DATE_COLUMNS,
    EXPECTED_BASELINE_COLUMN_COUNT,
    EXPECTED_COLUMNS_BASELINE,
    FAIL_ON_EMPTY_FILES,
    INVALID_TEXT_VALUES,
    KEY_COLUMNS_PRIMARY,
    KEY_COLUMNS_SECONDARY,
    MAX_EVIDENCE_ROWS,
    MODE_NAME,
    TIMESTAMP_COLUMN,
)


class MembershipsDeltaValidator:
    def __init__(self, snapshot_context: SnapshotContext | None = None) -> None:
        self.checks: List[CheckResult] = []
        self.evidence: Dict[str, pd.DataFrame] = {}
        self.snapshot_context = snapshot_context or SnapshotContext()

    def add_check(
        self,
        check_id: str,
        check_name: str,
        status: str,
        category: str,
        country: str = "ALL",
        field: str = "",
        expected: str = "",
        actual: str = "",
        details: str = "",
        evidence_file: str = "",
    ) -> None:
        self.checks.append(
            CheckResult(
                check_id=check_id,
                check_name=check_name,
                status=status,
                category=category,
                dataset=f"{DATASET_NAME} - {MODE_NAME}",
                country=country,
                field=field,
                expected=expected,
                actual=actual,
                details=details,
                evidence_file=evidence_file,
            )
        )

    @staticmethod
    def _trim_series(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip()

    def _normalized_rows(self, df: pd.DataFrame) -> List[Tuple[str, ...]]:
        work = df.copy()
        for col in EXPECTED_COLUMNS_BASELINE:
            if col in work.columns:
                work[col] = self._trim_series(work[col])
        return list(map(tuple, work[EXPECTED_COLUMNS_BASELINE].to_records(index=False)))

    def validate(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> ValidationRun:
        print("  [1/11] Snapshot validation...")
        self._validate_snapshot()

        print("  [2/11] Schema validation...")
        self._validate_schema(domo_df, inf_df)

        print("  [3/11] Empty / universe validation...")
        self._validate_empty_files(domo_df, inf_df)

        print("  [4/11] Record count validation...")
        self._validate_row_count(domo_df, inf_df)

        print("  [5/11] Key reconciliation...")
        self._validate_keys(domo_df, inf_df)

        print("  [6/11] Country scope validation...")
        self._validate_country_scope(domo_df, inf_df)

        print("  [7/11] Null / blank validation...")
        self._validate_null_blank(inf_df)

        print("  [8/11] Date / boolean behavior...")
        self._validate_dates_and_booleans(inf_df)

        print("  [9/11] Timestamp validation...")
        self._validate_timestamp(domo_df, inf_df)

        print("  [10/11] Full row reconciliation...")
        self._validate_full_rows(domo_df, inf_df)

        print("  [11/11] Overall decision...")
        self._validate_overall()

        summary = self._build_summary(domo_df, inf_df)
        print("  Validation summary built.")
        return ValidationRun(summary=summary, checks=self.checks, dataframes=self.evidence)

    def _validate_snapshot(self) -> None:
        domo_snapshot = self.snapshot_context.domo_snapshot.strip()
        inf_snapshot = self.snapshot_context.informatica_snapshot.strip()

        if not domo_snapshot and not inf_snapshot:
            self.add_check(
                "MEM-DELTA-001",
                "Verify Delta snapshot values are documented",
                "WARNING",
                "File Control",
                field="Snapshot / comparison window",
                expected="Matching snapshot values are recommended",
                actual="No snapshot values were provided",
            )
            return

        self.add_check(
            "MEM-DELTA-001",
            "Verify Delta snapshot values are documented",
            "PASS" if domo_snapshot == inf_snapshot else "FAIL",
            "File Control",
            field="Snapshot / comparison window",
            expected=domo_snapshot or "Value expected",
            actual=inf_snapshot or "Missing value",
        )

    def _validate_schema(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        domo_cols = list(domo_df.columns)
        inf_cols = list(inf_df.columns)

        self.add_check(
            "MEM-DELTA-002A",
            "Verify Delta baseline schema definition is complete",
            "PASS" if len(EXPECTED_COLUMNS_BASELINE) == EXPECTED_BASELINE_COLUMN_COUNT else "FAIL",
            "Schema",
            field="Baseline configuration",
            expected=str(EXPECTED_BASELINE_COLUMN_COUNT),
            actual=str(len(EXPECTED_COLUMNS_BASELINE)),
        )

        missing_domo = [c for c in EXPECTED_COLUMNS_BASELINE if c not in domo_cols]
        missing_inf = [c for c in EXPECTED_COLUMNS_BASELINE if c not in inf_cols]
        extra_domo = [c for c in domo_cols if c not in EXPECTED_COLUMNS_BASELINE]
        extra_inf = [c for c in inf_cols if c not in EXPECTED_COLUMNS_BASELINE]

        self.add_check(
            "MEM-DELTA-002B",
            "Verify Delta contains all expected columns",
            "PASS" if not missing_domo and not missing_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="All expected columns present",
            actual=f"Missing in Domo={len(missing_domo)} | Missing in Informatica={len(missing_inf)}",
        )

        self.add_check(
            "MEM-DELTA-002C",
            "Verify Delta does not contain unexpected columns",
            "PASS" if not extra_domo and not extra_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="No extra columns",
            actual=f"Extra in Domo={len(extra_domo)} | Extra in Informatica={len(extra_inf)}",
        )

        self.add_check(
            "MEM-DELTA-002D",
            "Verify Delta preserves exact column order",
            "PASS" if domo_cols == EXPECTED_COLUMNS_BASELINE and inf_cols == EXPECTED_COLUMNS_BASELINE else "FAIL",
            "Schema",
            field="All columns",
            expected="Exact baseline order",
            actual=f"Domo ok={domo_cols == EXPECTED_COLUMNS_BASELINE} | Informatica ok={inf_cols == EXPECTED_COLUMNS_BASELINE}",
        )

    def _validate_empty_files(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        self.add_check(
            "MEM-DELTA-003",
            "Verify Delta files contain data rows",
            "PASS" if (len(domo_df) > 0 and len(inf_df) > 0) or not FAIL_ON_EMPTY_FILES else "FAIL",
            "Volume",
            field="All rows",
            expected="Both files contain at least one row",
            actual=f"Domo={len(domo_df)} | Informatica={len(inf_df)}",
        )

    def _validate_row_count(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        self.add_check(
            "MEM-DELTA-004",
            "Verify Delta row count matches between Domo and Informatica",
            "PASS" if len(domo_df) == len(inf_df) else "FAIL",
            "Volume",
            field="All rows",
            expected=str(len(domo_df)),
            actual=str(len(inf_df)),
        )

    def _validate_keys(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        for idx, col in enumerate(KEY_COLUMNS_PRIMARY + KEY_COLUMNS_SECONDARY, start=1):
            check_id = f"MEM-DELTA-005{idx}"
            if col not in domo_df.columns or col not in inf_df.columns:
                self.add_check(
                    check_id,
                    f"Verify Delta key population for {col}",
                    "FAIL",
                    "Population",
                    field=col,
                    expected="Column exists in both files",
                    actual="Missing in one or both files",
                )
                continue

            domo_vals = set(self._trim_series(domo_df[col]))
            inf_vals = set(self._trim_series(inf_df[col]))
            only_domo = sorted(domo_vals - inf_vals)
            only_inf = sorted(inf_vals - domo_vals)

            self.add_check(
                check_id,
                f"Verify Delta key population for {col}",
                "PASS" if not only_domo and not only_inf else "FAIL",
                "Population",
                field=col,
                expected="Exact set equality",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)}",
            )

    def _validate_country_scope(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        for col in COUNTRY_COLUMNS:
            if col not in domo_df.columns or col not in inf_df.columns:
                continue

            domo_vals = set(self._trim_series(domo_df[col]))
            inf_vals = set(self._trim_series(inf_df[col]))
            only_domo = sorted(domo_vals - inf_vals)
            only_inf = sorted(inf_vals - domo_vals)

            self.add_check(
                f"MEM-DELTA-006-{col}",
                f"Verify Delta country scope for {col}",
                "PASS" if not only_domo and not only_inf else "FAIL",
                "Country Content",
                field=col,
                expected="Same country population in both files",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)}",
            )

    def _validate_null_blank(self, inf_df: pd.DataFrame) -> None:
        required = KEY_COLUMNS_PRIMARY + [TIMESTAMP_COLUMN]
        for col in required:
            if col not in inf_df.columns:
                self.add_check(
                    f"MEM-DELTA-007-{col}",
                    f"Verify {col} is not null or blank",
                    "FAIL",
                    "Quality",
                    field=col,
                    expected="Column exists",
                    actual="Column missing",
                )
                continue

            raw = inf_df[col]
            trimmed = self._trim_series(raw)
            issues = int(raw.isna().sum()) + int(trimmed.eq("").sum()) + int(trimmed.str.lower().isin(INVALID_TEXT_VALUES).sum())

            self.add_check(
                f"MEM-DELTA-007-{col}",
                f"Verify {col} is not null or blank",
                "PASS" if issues == 0 else "FAIL",
                "Quality",
                field=col,
                expected="No nulls or blanks",
                actual=f"Issues found={issues}",
            )

    def _validate_dates_and_booleans(self, inf_df: pd.DataFrame) -> None:
        for col in DATE_COLUMNS:
            if col not in inf_df.columns:
                continue
            parsed = pd.to_datetime(self._trim_series(inf_df[col]).replace("", pd.NA), errors="coerce")
            invalid = int(parsed.isna().sum()) - int(self._trim_series(inf_df[col]).eq("").sum())
            self.add_check(
                f"MEM-DELTA-008-DATE-{col}",
                f"Verify date behavior for {col}",
                "PASS" if invalid == 0 else "FAIL",
                "Quality",
                field=col,
                expected="Parseable date values",
                actual=f"Invalid values={invalid}",
            )

        for col in BOOLEAN_LIKE_COLUMNS:
            if col not in inf_df.columns:
                continue
            vals = set(self._trim_series(inf_df[col]).str.lower())
            allowed = {"", "true", "false", "0", "1", "y", "n", "yes", "no"}
            invalid = sorted(vals - allowed)
            self.add_check(
                f"MEM-DELTA-008-BOOL-{col}",
                f"Verify boolean-like behavior for {col}",
                "PASS" if not invalid else "FAIL",
                "Quality",
                field=col,
                expected="Allowed boolean-like values",
                actual=", ".join(invalid[:10]) if invalid else "No invalid values",
            )

    def _validate_timestamp(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if TIMESTAMP_COLUMN not in domo_df.columns or TIMESTAMP_COLUMN not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-009",
                "Verify Delta timestamp range consistency",
                "FAIL",
                "Quality",
                field=TIMESTAMP_COLUMN,
                expected="Timestamp column exists in both files",
                actual="Missing in one or both files",
            )
            return

        domo_ts = pd.to_datetime(self._trim_series(domo_df[TIMESTAMP_COLUMN]).replace("", pd.NA), errors="coerce").dropna()
        inf_ts = pd.to_datetime(self._trim_series(inf_df[TIMESTAMP_COLUMN]).replace("", pd.NA), errors="coerce").dropna()

        if domo_ts.empty or inf_ts.empty:
            self.add_check(
                "MEM-DELTA-009",
                "Verify Delta timestamp range consistency",
                "FAIL",
                "Quality",
                field=TIMESTAMP_COLUMN,
                expected="Valid timestamps in both files",
                actual="One or both files do not contain valid timestamps",
            )
            return

        self.add_check(
            "MEM-DELTA-009",
            "Verify Delta timestamp range consistency",
            "PASS" if (domo_ts.min() == inf_ts.min() and domo_ts.max() == inf_ts.max()) else "FAIL",
            "Quality",
            field=TIMESTAMP_COLUMN,
            expected=f"{domo_ts.min()} -> {domo_ts.max()}",
            actual=f"{inf_ts.min()} -> {inf_ts.max()}",
        )

    def _validate_full_rows(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if any(c not in domo_df.columns for c in EXPECTED_COLUMNS_BASELINE) or any(c not in inf_df.columns for c in EXPECTED_COLUMNS_BASELINE):
            self.add_check(
                "MEM-DELTA-010",
                "Verify full Delta row reconciliation",
                "FAIL",
                "Overall Reconciliation",
                field="All baseline columns",
                expected="All baseline columns exist",
                actual="Schema missing columns",
            )
            return

        domo_counts = Counter(self._normalized_rows(domo_df))
        inf_counts = Counter(self._normalized_rows(inf_df))

        diffs = []
        for row in set(domo_counts) | set(inf_counts):
            if domo_counts.get(row, 0) != inf_counts.get(row, 0):
                diffs.append(row)

        self.add_check(
            "MEM-DELTA-010",
            "Verify full Delta row reconciliation",
            "PASS" if not diffs else "FAIL",
            "Overall Reconciliation",
            field="All baseline columns",
            expected="Exact normalized row multiset equivalence",
            actual=f"Different rows={len(diffs)}",
        )

    def _validate_overall(self) -> None:
        fails = sum(1 for c in self.checks if c.status == "FAIL")
        self.add_check(
            "MEM-DELTA-999",
            "Verify full Delta output is equivalent between Domo and Informatica",
            "PASS" if fails == 0 else "FAIL",
            "Overall Reconciliation",
            field="All checks",
            expected="No blocking failures",
            actual=f"Failures={fails}",
        )

    def _build_summary(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> Dict[str, object]:
        status_counts = Counter(c.status for c in self.checks)
        return {
            "dataset": f"{DATASET_NAME} - {MODE_NAME}",
            "domo_rows": len(domo_df),
            "informatica_rows": len(inf_df),
            "domo_columns": len(domo_df.columns),
            "informatica_columns": len(inf_df.columns),
            "total_checks": len(self.checks),
            "passed": status_counts.get("PASS", 0),
            "failed": status_counts.get("FAIL", 0),
            "warnings": status_counts.get("WARNING", 0),
            "overall_status": "FAIL" if status_counts.get("FAIL", 0) else "PASS",
        }