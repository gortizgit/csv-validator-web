from __future__ import annotations

from collections import Counter
from typing import Dict, List, Tuple

import pandas as pd

from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun
from memberships_validator.config.memberships_delta_config import (
    BOOLEAN_LIKE_COLUMNS,
    COUNTRY_ALIASES,
    COUNTRY_CANONICAL_CODES,
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

    def _normalize_country_value(self, value: object) -> str:
        if value is None:
            return ""
        raw = str(value).strip()
        if not raw:
            return ""
        normalized_key = raw.upper().replace("_", " ").replace("-", " ")
        normalized_key = " ".join(normalized_key.split())
        normalized_key_compact = normalized_key.replace(" ", "")
        return COUNTRY_ALIASES.get(normalized_key, COUNTRY_ALIASES.get(normalized_key_compact, raw.upper()))

    def _resolve_row_country(self, row: Dict[str, object]) -> str:
        normalized_values = []
        for col in COUNTRY_COLUMNS:
            if col in row:
                normalized = self._normalize_country_value(row.get(col, ""))
                if normalized:
                    normalized_values.append(normalized)

        if not normalized_values:
            return ""

        counts = Counter(normalized_values)
        return counts.most_common(1)[0][0]

    def _country_alignment_issues(self, row: Dict[str, object], expected_country: str) -> List[str]:
        issues: List[str] = []
        for col in COUNTRY_COLUMNS:
            if col not in row:
                issues.append(f"{col} missing")
                continue

            raw_value = row.get(col, "")
            normalized = self._normalize_country_value(raw_value)

            if not normalized:
                issues.append(f"{col} blank")
            elif normalized not in COUNTRY_CANONICAL_CODES:
                issues.append(f"{col} invalid value={raw_value}")
            elif normalized != expected_country:
                issues.append(f"{col} resolves to {normalized}")

        return issues

    def _build_key(self, row: Dict[str, object], key_columns: List[str]) -> Tuple[str, ...]:
        return tuple(self._trim_series(pd.Series([row.get(col, "")])).iloc[0] for col in key_columns)

    def _normalized_rows(self, df: pd.DataFrame) -> List[Tuple[str, ...]]:
        work = df.copy()
        for col in EXPECTED_COLUMNS_BASELINE:
            if col in work.columns:
                work[col] = self._trim_series(work[col])
        return list(map(tuple, work[EXPECTED_COLUMNS_BASELINE].to_records(index=False)))

    def _build_primary_index(self, df: pd.DataFrame) -> Dict[Tuple[str, ...], Dict[str, object]]:
        deduped = df.drop_duplicates(subset=KEY_COLUMNS_PRIMARY, keep="first").copy()
        records = deduped.to_dict(orient="records")
        indexed: Dict[Tuple[str, ...], Dict[str, object]] = {}
        for row in records:
            indexed[self._build_key(row, KEY_COLUMNS_PRIMARY)] = row
        return indexed

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

        print("  [6/11] Strict country validation...")
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

        if missing_domo:
            self.evidence["memberships_delta_missing_columns_domo"] = pd.DataFrame({"column": missing_domo})
        if missing_inf:
            self.evidence["memberships_delta_missing_columns_infa"] = pd.DataFrame({"column": missing_inf})
        if extra_domo:
            self.evidence["memberships_delta_extra_columns_domo"] = pd.DataFrame({"column": extra_domo})
        if extra_inf:
            self.evidence["memberships_delta_extra_columns_infa"] = pd.DataFrame({"column": extra_inf})

        self.add_check(
            "MEM-DELTA-002B",
            "Verify Delta contains all expected columns",
            "PASS" if not missing_domo and not missing_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="All expected columns present",
            actual=f"Missing in Domo={len(missing_domo)} | Missing in Informatica={len(missing_inf)}",
            evidence_file="memberships_delta_missing_columns_infa.csv" if missing_inf else "",
        )

        self.add_check(
            "MEM-DELTA-002C",
            "Verify Delta does not contain unexpected columns",
            "PASS" if not extra_domo and not extra_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="No extra columns",
            actual=f"Extra in Domo={len(extra_domo)} | Extra in Informatica={len(extra_inf)}",
            evidence_file="memberships_delta_extra_columns_infa.csv" if extra_inf else "",
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

            if only_domo or only_inf:
                self.evidence[f"memberships_delta_key_diff_{col}"] = pd.DataFrame(
                    [{"source": "DOMO_ONLY", col: x} for x in only_domo[:MAX_EVIDENCE_ROWS]]
                    + [{"source": "INFA_ONLY", col: x} for x in only_inf[:MAX_EVIDENCE_ROWS]]
                )

            self.add_check(
                check_id,
                f"Verify Delta key population for {col}",
                "PASS" if not only_domo and not only_inf else "FAIL",
                "Population",
                field=col,
                expected="Exact set equality",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)}",
                evidence_file=f"memberships_delta_key_diff_{col}.csv" if only_domo or only_inf else "",
            )

    def _validate_country_scope(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        missing_country_cols = [c for c in COUNTRY_COLUMNS if c not in domo_df.columns or c not in inf_df.columns]
        if missing_country_cols:
            self.add_check(
                "MEM-DELTA-006A",
                "Verify strict Delta country validation prerequisites exist",
                "FAIL",
                "Country Content",
                field="; ".join(COUNTRY_COLUMNS),
                expected="All country columns exist in both files",
                actual=f"Missing columns={len(missing_country_cols)}",
                details=", ".join(missing_country_cols),
            )
            return

        print("    Preparing indexed data for strict country validation...")
        domo_indexed = self._build_primary_index(domo_df)
        inf_indexed = self._build_primary_index(inf_df)
        common_keys = sorted(set(domo_indexed.keys()) & set(inf_indexed.keys()))
        print(f"    Common unique membership keys for country checks: {len(common_keys):,}")

        field_diff_records: List[Dict[str, object]] = []
        internal_alignment_issues: List[Dict[str, object]] = []
        country_count_diffs: List[Dict[str, object]] = []
        invalid_country_values: List[Dict[str, object]] = []

        for idx, country in enumerate(COUNTRY_CANONICAL_CODES, start=1):
            print(f"    [{idx}/{len(COUNTRY_CANONICAL_CODES)}] Country {country}...")

            domo_country_keys = {
                key for key, row in domo_indexed.items() if self._resolve_row_country(row) == country
            }
            inf_country_keys = {
                key for key, row in inf_indexed.items() if self._resolve_row_country(row) == country
            }

            count_match = len(domo_country_keys) == len(inf_country_keys)
            if not count_match:
                country_count_diffs.append(
                    {
                        "country": country,
                        "domo_count": len(domo_country_keys),
                        "informatica_count": len(inf_country_keys),
                    }
                )

            self.add_check(
                f"MEM-DELTA-{country}-COUNT",
                f"Verify Delta country universe count for {country}",
                "PASS" if count_match else "FAIL",
                "Country Content",
                country=country,
                field="Country universe",
                expected=f"Domo count={len(domo_country_keys)}",
                actual=f"Informatica count={len(inf_country_keys)}",
                details="Counts are based on normalized country resolution across all country fields.",
                evidence_file="memberships_delta_country_count_diffs.csv" if not count_match else "",
            )

            common_country_keys = sorted(domo_country_keys & inf_country_keys)

            for country_col in COUNTRY_COLUMNS:
                mismatch_count = 0
                invalid_count = 0

                for key in common_country_keys:
                    domo_row = domo_indexed[key]
                    inf_row = inf_indexed[key]

                    domo_raw = str(domo_row.get(country_col, ""))
                    inf_raw = str(inf_row.get(country_col, ""))

                    domo_norm = self._normalize_country_value(domo_raw)
                    inf_norm = self._normalize_country_value(inf_raw)

                    if domo_norm != inf_norm:
                        mismatch_count += 1
                        if len(field_diff_records) < MAX_EVIDENCE_ROWS:
                            field_diff_records.append(
                                {
                                    "Membership_Number": key[0] if len(key) > 0 else "",
                                    "Account_Number_Full": key[1] if len(key) > 1 else "",
                                    "country": country,
                                    "field": country_col,
                                    "domo_raw": domo_raw,
                                    "informatica_raw": inf_raw,
                                    "domo_normalized": domo_norm,
                                    "informatica_normalized": inf_norm,
                                }
                            )

                    if inf_norm and inf_norm not in COUNTRY_CANONICAL_CODES:
                        invalid_count += 1
                        if len(invalid_country_values) < MAX_EVIDENCE_ROWS:
                            invalid_country_values.append(
                                {
                                    "Membership_Number": key[0] if len(key) > 0 else "",
                                    "Account_Number_Full": key[1] if len(key) > 1 else "",
                                    "country": country,
                                    "field": country_col,
                                    "raw_value": inf_raw,
                                    "normalized_value": inf_norm,
                                }
                            )

                self.add_check(
                    f"MEM-DELTA-{country}-{country_col}",
                    f"Verify that {country_col} is preserved exactly for {country}",
                    "PASS" if mismatch_count == 0 else "FAIL",
                    "Country Content",
                    country=country,
                    field=country_col,
                    expected="Exact normalized country equivalence for matching membership keys",
                    actual="Matched" if mismatch_count == 0 else f"record_differences={mismatch_count}",
                    details=(
                        f"{invalid_count} invalid normalized values detected in Informatica."
                        if invalid_count > 0
                        else ""
                    ),
                    evidence_file=(
                        "memberships_delta_country_field_diffs.csv"
                        if mismatch_count > 0
                        else ("memberships_delta_invalid_country_values.csv" if invalid_count > 0 else "")
                    ),
                )

            alignment_failures = 0
            for key in common_country_keys:
                inf_row = inf_indexed[key]
                issues = self._country_alignment_issues(inf_row, country)
                if issues:
                    alignment_failures += 1
                    if len(internal_alignment_issues) < MAX_EVIDENCE_ROWS:
                        internal_alignment_issues.append(
                            {
                                "Membership_Number": key[0] if len(key) > 0 else "",
                                "Account_Number_Full": key[1] if len(key) > 1 else "",
                                "country": country,
                                "issues": " | ".join(issues),
                                **{col: inf_row.get(col, "") for col in COUNTRY_COLUMNS},
                            }
                        )

            self.add_check(
                f"MEM-DELTA-{country}-ALIGN",
                f"Verify internal country-field alignment for {country}",
                "PASS" if alignment_failures == 0 else "FAIL",
                "Country Content",
                country=country,
                field="; ".join(COUNTRY_COLUMNS),
                expected="All populated country fields resolve to the same country code",
                actual="Matched" if alignment_failures == 0 else f"row_alignment_failures={alignment_failures}",
                details="Country fields are normalized and compared row by row.",
                evidence_file="memberships_delta_country_alignment_issues.csv" if alignment_failures > 0 else "",
            )

        if field_diff_records:
            self.evidence["memberships_delta_country_field_diffs"] = pd.DataFrame(field_diff_records)
        if internal_alignment_issues:
            self.evidence["memberships_delta_country_alignment_issues"] = pd.DataFrame(internal_alignment_issues)
        if country_count_diffs:
            self.evidence["memberships_delta_country_count_diffs"] = pd.DataFrame(country_count_diffs)
        if invalid_country_values:
            self.evidence["memberships_delta_invalid_country_values"] = pd.DataFrame(invalid_country_values)

        overall_validity_issues = 0
        for row in inf_df.to_dict(orient="records"):
            for col in COUNTRY_COLUMNS:
                normalized = self._normalize_country_value(row.get(col, ""))
                if normalized and normalized not in COUNTRY_CANONICAL_CODES:
                    overall_validity_issues += 1

        self.add_check(
            "MEM-DELTA-006Z",
            "Verify all Delta country fields contain recognized country values",
            "PASS" if overall_validity_issues == 0 else "FAIL",
            "Country Content",
            field="; ".join(COUNTRY_COLUMNS),
            expected="All populated country fields resolve to supported countries",
            actual="No invalid values" if overall_validity_issues == 0 else f"invalid_values={overall_validity_issues}",
            evidence_file="memberships_delta_invalid_country_values.csv" if overall_validity_issues > 0 else "",
        )

    def _validate_null_blank(self, inf_df: pd.DataFrame) -> None:
        required = list(dict.fromkeys(KEY_COLUMNS_PRIMARY + KEY_COLUMNS_SECONDARY + COUNTRY_COLUMNS + [TIMESTAMP_COLUMN]))
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

        if diffs:
            self.evidence["memberships_delta_full_row_diffs"] = pd.DataFrame(
                [dict(zip(EXPECTED_COLUMNS_BASELINE, row)) for row in diffs[:MAX_EVIDENCE_ROWS]]
            )

        self.add_check(
            "MEM-DELTA-010",
            "Verify full Delta row reconciliation",
            "PASS" if not diffs else "FAIL",
            "Overall Reconciliation",
            field="All baseline columns",
            expected="Exact normalized row multiset equivalence",
            actual=f"Different rows={len(diffs)}",
            evidence_file="memberships_delta_full_row_diffs.csv" if diffs else "",
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

        domo_country_population = sorted(
            {
                self._normalize_country_value(value)
                for col in COUNTRY_COLUMNS
                if col in domo_df.columns
                for value in self._trim_series(domo_df[col]).tolist()
                if self._normalize_country_value(value)
            }
        )
        inf_country_population = sorted(
            {
                self._normalize_country_value(value)
                for col in COUNTRY_COLUMNS
                if col in inf_df.columns
                for value in self._trim_series(inf_df[col]).tolist()
                if self._normalize_country_value(value)
            }
        )

        return {
            "dataset": f"{DATASET_NAME} - {MODE_NAME}",
            "domo_rows": len(domo_df),
            "informatica_rows": len(inf_df),
            "domo_columns": len(domo_df.columns),
            "informatica_columns": len(inf_df.columns),
            "domo_country_population": ", ".join(domo_country_population),
            "informatica_country_population": ", ".join(inf_country_population),
            "total_checks": len(self.checks),
            "passed": status_counts.get("PASS", 0),
            "failed": status_counts.get("FAIL", 0),
            "warnings": status_counts.get("WARNING", 0),
            "overall_status": "FAIL" if status_counts.get("FAIL", 0) else "PASS",
        }