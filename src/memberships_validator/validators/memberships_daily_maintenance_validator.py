from __future__ import annotations

from collections import Counter
from typing import Dict, List, Tuple

import pandas as pd

from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun
from memberships_validator.config.memberships_daily_maintenance_config import (
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
)


class MembershipsDailyMaintenanceValidator:
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
        print("  [1/10] Snapshot validation...")
        self._validate_snapshot()

        print("  [2/10] Schema validation...")
        self._validate_schema(domo_df, inf_df)

        print("  [3/10] Empty / universe validation...")
        self._validate_empty_files(domo_df, inf_df)

        print("  [4/10] Record count validation...")
        self._validate_row_count(domo_df, inf_df)

        print("  [5/10] Key reconciliation...")
        self._validate_keys(domo_df, inf_df)

        print("  [6/10] Country scope validation...")
        self._validate_country_scope(domo_df, inf_df)

        print("  [7/10] Null / blank validation...")
        self._validate_null_blank(inf_df)

        print("  [8/10] Date / boolean behavior...")
        self._validate_dates_and_booleans(inf_df)

        print("  [9/10] Full row reconciliation...")
        self._validate_full_rows(domo_df, inf_df)

        print("  [10/10] Overall decision...")
        self._validate_overall()

        summary = self._build_summary(domo_df, inf_df)
        print("  Validation summary built.")
        return ValidationRun(summary=summary, checks=self.checks, dataframes=self.evidence)

    def _validate_snapshot(self) -> None:
        domo_snapshot = self.snapshot_context.domo_snapshot.strip()
        inf_snapshot = self.snapshot_context.informatica_snapshot.strip()

        if not domo_snapshot and not inf_snapshot:
            self.add_check(
                "MEM-DAILY-001",
                "Verify Daily Maintenance snapshot values are documented",
                "WARNING",
                "File Control",
                field="Snapshot / comparison window",
                expected="Matching snapshot values are recommended",
                actual="No snapshot values were provided",
                details="Validation can run without snapshot values, but documentation is recommended.",
            )
            return

        self.add_check(
            "MEM-DAILY-001",
            "Verify Daily Maintenance snapshot values are documented",
            "PASS" if domo_snapshot == inf_snapshot else "FAIL",
            "File Control",
            field="Snapshot / comparison window",
            expected=domo_snapshot or "Value expected",
            actual=inf_snapshot or "Missing value",
            details="Both snapshot values should normally match.",
        )

    def _validate_schema(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        domo_cols = list(domo_df.columns)
        inf_cols = list(inf_df.columns)

        self.add_check(
            "MEM-DAILY-002A",
            "Verify Daily Maintenance baseline schema definition is complete",
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

        if missing_domo:
            self.evidence["memberships_daily_missing_columns_domo"] = pd.DataFrame({"column": missing_domo})
        if missing_inf:
            self.evidence["memberships_daily_missing_columns_infa"] = pd.DataFrame({"column": missing_inf})
        if extra_domo:
            self.evidence["memberships_daily_extra_columns_domo"] = pd.DataFrame({"column": extra_domo})
        if extra_inf:
            self.evidence["memberships_daily_extra_columns_infa"] = pd.DataFrame({"column": extra_inf})

        self.add_check(
            "MEM-DAILY-002B",
            "Verify Daily Maintenance contains all expected columns",
            "PASS" if not missing_domo and not missing_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="All expected columns present in both files",
            actual=f"Missing in Domo={len(missing_domo)} | Missing in Informatica={len(missing_inf)}",
            evidence_file="memberships_daily_missing_columns_infa.csv" if missing_inf else "",
        )

        self.add_check(
            "MEM-DAILY-002C",
            "Verify Daily Maintenance does not contain unexpected columns",
            "PASS" if not extra_domo and not extra_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="No extra columns",
            actual=f"Extra in Domo={len(extra_domo)} | Extra in Informatica={len(extra_inf)}",
            evidence_file="memberships_daily_extra_columns_infa.csv" if extra_inf else "",
        )

        self.add_check(
            "MEM-DAILY-002D",
            "Verify Daily Maintenance preserves exact column order",
            "PASS" if domo_cols == EXPECTED_COLUMNS_BASELINE and inf_cols == EXPECTED_COLUMNS_BASELINE else "FAIL",
            "Schema",
            field="All columns",
            expected="Exact baseline order",
            actual=f"Domo ok={domo_cols == EXPECTED_COLUMNS_BASELINE} | Informatica ok={inf_cols == EXPECTED_COLUMNS_BASELINE}",
        )

    def _validate_empty_files(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        self.add_check(
            "MEM-DAILY-003",
            "Verify Daily Maintenance files contain data rows",
            "PASS" if (len(domo_df) > 0 and len(inf_df) > 0) or not FAIL_ON_EMPTY_FILES else "FAIL",
            "Volume",
            field="All rows",
            expected="Both files contain at least one row",
            actual=f"Domo={len(domo_df)} | Informatica={len(inf_df)}",
        )

    def _validate_row_count(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        self.add_check(
            "MEM-DAILY-004",
            "Verify Daily Maintenance row count matches between Domo and Informatica",
            "PASS" if len(domo_df) == len(inf_df) else "FAIL",
            "Volume",
            field="All rows",
            expected=str(len(domo_df)),
            actual=str(len(inf_df)),
        )

    def _validate_keys(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        for idx, col in enumerate(KEY_COLUMNS_PRIMARY + KEY_COLUMNS_SECONDARY, start=1):
            check_id = f"MEM-DAILY-005{idx}"
            if col not in domo_df.columns or col not in inf_df.columns:
                self.add_check(
                    check_id,
                    f"Verify Daily Maintenance key population for {col}",
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

            if only_domo or only_inf:
                self.evidence[f"memberships_daily_key_diff_{col}"] = pd.DataFrame(
                    [{"source": "DOMO_ONLY", col: x} for x in only_domo[:MAX_EVIDENCE_ROWS]]
                    + [{"source": "INFA_ONLY", col: x} for x in only_inf[:MAX_EVIDENCE_ROWS]]
                )

            self.add_check(
                check_id,
                f"Verify Daily Maintenance key population for {col}",
                "PASS" if not only_domo and not only_inf else "FAIL",
                "Population",
                field=col,
                expected="Exact set equality",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)}",
                evidence_file=f"memberships_daily_key_diff_{col}.csv" if only_domo or only_inf else "",
            )

    def _validate_country_scope(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        for col in COUNTRY_COLUMNS:
            if col not in domo_df.columns or col not in inf_df.columns:
                self.add_check(
                    f"MEM-DAILY-006-{col}",
                    f"Verify country scope for {col}",
                    "FAIL",
                    "Country Content",
                    field=col,
                    expected="Country column exists in both files",
                    actual="Missing in one or both files",
                )
                continue

            domo_vals = set(self._trim_series(domo_df[col]))
            inf_vals = set(self._trim_series(inf_df[col]))
            only_domo = sorted(domo_vals - inf_vals)
            only_inf = sorted(inf_vals - domo_vals)

            self.add_check(
                f"MEM-DAILY-006-{col}",
                f"Verify country scope for {col}",
                "PASS" if not only_domo and not only_inf else "FAIL",
                "Country Content",
                field=col,
                expected="Same country population in both files",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)}",
            )

    def _validate_null_blank(self, inf_df: pd.DataFrame) -> None:
        required = KEY_COLUMNS_PRIMARY + ["Membership_Number"]
        for col in required:
            if col not in inf_df.columns:
                self.add_check(
                    f"MEM-DAILY-007-{col}",
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
                f"MEM-DAILY-007-{col}",
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
                f"MEM-DAILY-008-DATE-{col}",
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
                f"MEM-DAILY-008-BOOL-{col}",
                f"Verify boolean-like behavior for {col}",
                "PASS" if not invalid else "FAIL",
                "Quality",
                field=col,
                expected="Allowed boolean-like values",
                actual=", ".join(invalid[:10]) if invalid else "No invalid values",
            )

    def _validate_full_rows(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if any(c not in domo_df.columns for c in EXPECTED_COLUMNS_BASELINE) or any(c not in inf_df.columns for c in EXPECTED_COLUMNS_BASELINE):
            self.add_check(
                "MEM-DAILY-009",
                "Verify full Daily Maintenance row reconciliation",
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

        if diffs:
            self.evidence["memberships_daily_full_row_diffs"] = pd.DataFrame(
                [dict(zip(EXPECTED_COLUMNS_BASELINE, row)) for row in diffs[:MAX_EVIDENCE_ROWS]]
            )

        self.add_check(
            "MEM-DAILY-009",
            "Verify full Daily Maintenance row reconciliation",
            "PASS" if not diffs else "FAIL",
            "Overall Reconciliation",
            field="All baseline columns",
            expected="Exact normalized row multiset equivalence",
            actual=f"Different rows={len(diffs)}",
            evidence_file="memberships_daily_full_row_diffs.csv" if diffs else "",
        )

    def _validate_overall(self) -> None:
        fails = sum(1 for c in self.checks if c.status == "FAIL")
        self.add_check(
            "MEM-DAILY-999",
            "Verify full Daily Maintenance output is equivalent between Domo and Informatica",
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