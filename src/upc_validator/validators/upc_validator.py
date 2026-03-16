from __future__ import annotations

from collections import Counter
from typing import Dict, List, Tuple

import pandas as pd

from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun
from upc_validator.config.upc_config import (
    DATASET_NAME,
    EXPECTED_BASELINE_COLUMN_COUNT,
    EXPECTED_COLUMNS_BASELINE,
    FAIL_ON_EMPTY_FILES,
    INVALID_TEXT_VALUES,
    KEY_ITEM_COLUMN,
    KEY_TIMESTAMP_COLUMN,
    KEY_UPC_COLUMN,
    MAX_EVIDENCE_ROWS,
    SNAPSHOT_CANDIDATE_COLUMNS,
    STRICT_SCHEMA_MODE,
    STRICT_SNAPSHOT_MODE,
)


class UpcValidator:
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
                dataset=DATASET_NAME,
                country=country,
                field=field,
                expected=expected,
                actual=actual,
                details=details,
                evidence_file=evidence_file,
            )
        )

    def validate(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> ValidationRun:
        print("  [1/11] Snapshot validation...")
        self._validate_snapshot(domo_df, inf_df)

        print("  [2/11] Schema validation...")
        self._validate_schema(domo_df, inf_df)

        print("  [3/11] Empty / universe validation...")
        self._validate_empty_and_universe_rules(domo_df, inf_df)

        print("  [4/11] Record count validation...")
        self._validate_record_count(domo_df, inf_df)

        print("  [5/11] Key population validation...")
        self._validate_key_population(domo_df, inf_df)

        print("  [6/11] Duplicate validation...")
        self._validate_duplicates(domo_df, inf_df)

        print("  [7/11] Null / blank validation...")
        self._validate_null_blank_rules(inf_df)

        print("  [8/11] Whitespace / text behavior validation...")
        self._validate_text_behavior(inf_df)

        print("  [9/11] Timestamp validation...")
        self._validate_timestamp_population_and_parse(domo_df, inf_df)

        print("  [10/11] Full row reconciliation...")
        self._validate_full_row_reconciliation(domo_df, inf_df)

        print("  [11/11] Overall decision...")
        self._validate_overall_decision()

        summary = self._build_summary(domo_df, inf_df)
        print("  Validation summary built.")
        return ValidationRun(summary=summary, checks=self.checks, dataframes=self.evidence)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(dtype="object")
        return df[col]

    @staticmethod
    def _normalize_text_series(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str)

    @staticmethod
    def _trim_series(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip()

    def _normalized_rows(self, df: pd.DataFrame) -> List[Tuple[str, ...]]:
        if any(col not in df.columns for col in EXPECTED_COLUMNS_BASELINE):
            return []

        work = df[EXPECTED_COLUMNS_BASELINE].copy()
        for col in EXPECTED_COLUMNS_BASELINE:
            work[col] = work[col].fillna("").astype(str).str.strip()

        return list(map(tuple, work.to_records(index=False)))

    # -------------------------------------------------------------------------
    # Snapshot
    # -------------------------------------------------------------------------
    def _validate_snapshot(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        manual_domo = self.snapshot_context.domo_snapshot.strip()
        manual_inf = self.snapshot_context.informatica_snapshot.strip()

        if manual_domo or manual_inf:
            status = "PASS" if manual_domo and manual_inf and manual_domo == manual_inf else "FAIL"
            self.add_check(
                "UPC-001",
                "Verify that Domo and Informatica UPC files belong to the same business snapshot",
                status,
                "File Control",
                field="Business date or approved comparison window",
                expected=manual_domo or "Manual snapshot value required",
                actual=manual_inf or "Missing Informatica snapshot value",
                details="Manual snapshot comparison was used.",
            )
            return

        common = [c for c in SNAPSHOT_CANDIDATE_COLUMNS if c in domo_df.columns and c in inf_df.columns]
        if not common:
            self.add_check(
                "UPC-001",
                "Verify that Domo and Informatica UPC files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date or approved comparison window",
                expected="Shared snapshot metadata or manual inputs",
                actual="No comparable snapshot metadata found",
                details="Snapshot provenance could not be verified from file contents alone.",
            )
            return

        mismatches = []
        multiplicity_issues = []

        for col in common:
            domo_vals = sorted(set(self._trim_series(domo_df[col]).tolist()))
            inf_vals = sorted(set(self._trim_series(inf_df[col]).tolist()))

            if STRICT_SNAPSHOT_MODE and (len(domo_vals) != 1 or len(inf_vals) != 1):
                multiplicity_issues.append(
                    {
                        "column": col,
                        "domo_distinct_values": len(domo_vals),
                        "informatica_distinct_values": len(inf_vals),
                        "domo_values_preview": " | ".join(map(str, domo_vals[:10])),
                        "informatica_values_preview": " | ".join(map(str, inf_vals[:10])),
                    }
                )

            if domo_vals != inf_vals:
                mismatches.append(
                    {
                        "column": col,
                        "domo_values": " | ".join(map(str, domo_vals[:10])),
                        "informatica_values": " | ".join(map(str, inf_vals[:10])),
                    }
                )

        if multiplicity_issues:
            self.evidence["upc_snapshot_multiplicity_issues"] = pd.DataFrame(multiplicity_issues)

        if mismatches:
            self.evidence["upc_snapshot_mismatches"] = pd.DataFrame(mismatches)
            self.add_check(
                "UPC-001",
                "Verify that Domo and Informatica UPC files belong to the same business snapshot",
                "FAIL",
                "File Control",
                field="Business date or approved comparison window",
                expected="Same snapshot metadata set",
                actual=f"{len(mismatches)} comparable snapshot column(s) differ",
                details="See upc_snapshot_mismatches.csv",
                evidence_file="upc_snapshot_mismatches.csv",
            )
            return

        if multiplicity_issues:
            self.add_check(
                "UPC-001",
                "Verify that Domo and Informatica UPC files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date or approved comparison window",
                expected="Exactly one distinct value per comparable metadata field",
                actual=f"{len(multiplicity_issues)} metadata field(s) contain multiple distinct values",
                details="Values match between sources, but multiplicity was detected.",
                evidence_file="upc_snapshot_multiplicity_issues.csv",
            )
            return

        self.add_check(
            "UPC-001",
            "Verify that Domo and Informatica UPC files belong to the same business snapshot",
            "PASS",
            "File Control",
            field="Business date or approved comparison window",
            expected="Same snapshot metadata set",
            actual=f"Matched on {len(common)} comparable field(s)",
        )

    # -------------------------------------------------------------------------
    # Schema
    # -------------------------------------------------------------------------
    def _validate_schema(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        domo_cols = list(domo_df.columns)
        inf_cols = list(inf_df.columns)
        baseline = EXPECTED_COLUMNS_BASELINE

        if STRICT_SCHEMA_MODE and len(baseline) != EXPECTED_BASELINE_COLUMN_COUNT:
            self.add_check(
                "UPC-002A",
                "Verify baseline definition contains the complete expected UPC schema",
                "FAIL",
                "Schema",
                field="Baseline configuration",
                expected=f"{EXPECTED_BASELINE_COLUMN_COUNT} columns",
                actual=f"{len(baseline)} columns",
                details="upc_config.py EXPECTED_COLUMNS_BASELINE must be corrected.",
            )
        else:
            self.add_check(
                "UPC-002A",
                "Verify baseline definition contains the complete expected UPC schema",
                "PASS",
                "Schema",
                field="Baseline configuration",
                expected=f"{EXPECTED_BASELINE_COLUMN_COUNT} columns",
                actual=f"{len(baseline)} columns",
            )

        missing_in_domo = [c for c in baseline if c not in domo_cols]
        missing_in_inf = [c for c in baseline if c not in inf_cols]
        extra_in_domo = [c for c in domo_cols if c not in baseline]
        extra_in_inf = [c for c in inf_cols if c not in baseline]

        if missing_in_domo:
            self.evidence["upc_missing_columns_in_domo"] = pd.DataFrame({"column": missing_in_domo})
        if missing_in_inf:
            self.evidence["upc_missing_columns_in_informatica"] = pd.DataFrame({"column": missing_in_inf})
        if extra_in_domo:
            self.evidence["upc_extra_columns_in_domo"] = pd.DataFrame({"column": extra_in_domo})
        if extra_in_inf:
            self.evidence["upc_extra_columns_in_informatica"] = pd.DataFrame({"column": extra_in_inf})

        self.add_check(
            "UPC-002",
            "Verify that Domo and Informatica preserve the complete approved UPC schema",
            "PASS" if not missing_in_domo and not missing_in_inf else "FAIL",
            "Schema",
            field="All columns",
            expected=f"All baseline columns present ({len(baseline)})",
            actual=f"Missing in Domo: {len(missing_in_domo)} | Missing in Informatica: {len(missing_in_inf)}",
            details=(
                f"Domo missing: {', '.join(missing_in_domo[:10])} | "
                f"Informatica missing: {', '.join(missing_in_inf[:10])}"
            ).strip(" |") if (missing_in_domo or missing_in_inf) else "No missing columns",
            evidence_file=(
                "upc_missing_columns_in_domo.csv"
                if missing_in_domo
                else ("upc_missing_columns_in_informatica.csv" if missing_in_inf else "")
            ),
        )

        self.add_check(
            "UPC-003",
            "Verify that no unexpected columns exist and header naming remains exact",
            "PASS" if not extra_in_domo and not extra_in_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="No extra columns and exact header naming",
            actual=f"Extra in Domo: {len(extra_in_domo)} | Extra in Informatica: {len(extra_in_inf)}",
            details=(
                f"Domo extras: {', '.join(extra_in_domo[:10])} | "
                f"Informatica extras: {', '.join(extra_in_inf[:10])}"
            ).strip(" |") if (extra_in_domo or extra_in_inf) else "No extra columns",
            evidence_file=(
                "upc_extra_columns_in_domo.csv"
                if extra_in_domo
                else ("upc_extra_columns_in_informatica.csv" if extra_in_inf else "")
            ),
        )

        exact_order_domo = domo_cols == baseline
        exact_order_inf = inf_cols == baseline

        if not exact_order_domo or not exact_order_inf:
            rows = []
            max_len = max(len(baseline), len(domo_cols), len(inf_cols))
            for idx in range(max_len):
                rows.append(
                    {
                        "position": idx + 1,
                        "expected": baseline[idx] if idx < len(baseline) else "",
                        "domo_actual": domo_cols[idx] if idx < len(domo_cols) else "",
                        "informatica_actual": inf_cols[idx] if idx < len(inf_cols) else "",
                    }
                )
            self.evidence["upc_column_order_differences"] = pd.DataFrame(rows)

        self.add_check(
            "UPC-004",
            "Verify that Domo and Informatica preserve the exact approved column order",
            "PASS" if exact_order_domo and exact_order_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="Exact baseline order",
            actual=f"Domo order ok={exact_order_domo} | Informatica order ok={exact_order_inf}",
            details="See upc_column_order_differences.csv" if not exact_order_domo or not exact_order_inf else "",
            evidence_file="upc_column_order_differences.csv" if not exact_order_domo or not exact_order_inf else "",
        )

    # -------------------------------------------------------------------------
    # Empty / universe
    # -------------------------------------------------------------------------
    def _validate_empty_and_universe_rules(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        domo_rows = len(domo_df)
        inf_rows = len(inf_df)

        if FAIL_ON_EMPTY_FILES:
            status = "PASS" if domo_rows > 0 and inf_rows > 0 else "FAIL"
            details = "Both files must contain at least one data row for strict reconciliation."
        else:
            status = "WARNING" if domo_rows == 0 or inf_rows == 0 else "PASS"
            details = "One or both files are empty."

        self.add_check(
            "UPC-005A",
            "Verify that both Domo and Informatica UPC files contain data rows",
            status,
            "Volume",
            field="All records",
            expected="Both sources contain at least 1 row",
            actual=f"Domo rows={domo_rows} | Informatica rows={inf_rows}",
            details=details,
        )

    # -------------------------------------------------------------------------
    # Record count
    # -------------------------------------------------------------------------
    def _validate_record_count(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        self.add_check(
            "UPC-005B",
            "Verify that the total record count in Informatica matches Domo for UPC",
            "PASS" if len(domo_df) == len(inf_df) else "FAIL",
            "Volume",
            field="All records",
            expected=str(len(domo_df)),
            actual=str(len(inf_df)),
            details="Direct full-file row count comparison.",
        )

    # -------------------------------------------------------------------------
    # Keys
    # -------------------------------------------------------------------------
    def _validate_key_population(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        key_cols = [KEY_UPC_COLUMN, KEY_ITEM_COLUMN]

        for idx, key_col in enumerate(key_cols, start=1):
            suffix = "A" if idx == 1 else "B"

            if key_col not in domo_df.columns or key_col not in inf_df.columns:
                self.add_check(
                    f"UPC-006{suffix}",
                    f"Verify that {key_col} population matches exactly between Domo and Informatica",
                    "FAIL",
                    "Population",
                    field=key_col,
                    expected=f"Column {key_col} exists in both files",
                    actual="Missing in one or both files",
                )
                continue

            domo_vals = set(self._trim_series(domo_df[key_col]).tolist())
            inf_vals = set(self._trim_series(inf_df[key_col]).tolist())

            only_domo = sorted(domo_vals - inf_vals)
            only_inf = sorted(inf_vals - domo_vals)

            if only_domo:
                self.evidence[f"upc_{key_col}_only_in_domo"] = pd.DataFrame({key_col: only_domo[:MAX_EVIDENCE_ROWS]})
            if only_inf:
                self.evidence[f"upc_{key_col}_only_in_informatica"] = pd.DataFrame({key_col: only_inf[:MAX_EVIDENCE_ROWS]})

            self.add_check(
                f"UPC-006{suffix}",
                f"Verify that {key_col} population matches exactly between Domo and Informatica",
                "PASS" if not only_domo and not only_inf else "FAIL",
                "Population",
                field=key_col,
                expected="Exact set equality",
                actual=f"only_in_domo={len(only_domo)} | only_in_informatica={len(only_inf)}",
                details="See evidence files if generated.",
                evidence_file=(
                    f"upc_{key_col}_only_in_domo.csv"
                    if only_domo
                    else (f"upc_{key_col}_only_in_informatica.csv" if only_inf else "")
                ),
            )

        if all(c in domo_df.columns for c in key_cols) and all(c in inf_df.columns for c in key_cols):
            domo_pairs = set(zip(self._trim_series(domo_df[KEY_UPC_COLUMN]), self._trim_series(domo_df[KEY_ITEM_COLUMN])))
            inf_pairs = set(zip(self._trim_series(inf_df[KEY_UPC_COLUMN]), self._trim_series(inf_df[KEY_ITEM_COLUMN])))

            only_domo_pairs = sorted(domo_pairs - inf_pairs)
            only_inf_pairs = sorted(inf_pairs - domo_pairs)

            if only_domo_pairs or only_inf_pairs:
                rows = []
                for upc_code, item_number in only_domo_pairs[:MAX_EVIDENCE_ROWS]:
                    rows.append(
                        {
                            "source": "DOMO_ONLY",
                            KEY_UPC_COLUMN: upc_code,
                            KEY_ITEM_COLUMN: item_number,
                        }
                    )
                for upc_code, item_number in only_inf_pairs[:MAX_EVIDENCE_ROWS]:
                    rows.append(
                        {
                            "source": "INFORMATICA_ONLY",
                            KEY_UPC_COLUMN: upc_code,
                            KEY_ITEM_COLUMN: item_number,
                        }
                    )
                self.evidence["upc_key_pair_differences"] = pd.DataFrame(rows)

            self.add_check(
                "UPC-006C",
                "Verify that UPC_Code and Item_Number combinations match exactly between Domo and Informatica",
                "PASS" if not only_domo_pairs and not only_inf_pairs else "FAIL",
                "Population",
                field=f"{KEY_UPC_COLUMN}; {KEY_ITEM_COLUMN}",
                expected="Exact pair set equality",
                actual=f"only_in_domo={len(only_domo_pairs)} | only_in_informatica={len(only_inf_pairs)}",
                details="Pair-level reconciliation across both files.",
                evidence_file="upc_key_pair_differences.csv" if only_domo_pairs or only_inf_pairs else "",
            )

    # -------------------------------------------------------------------------
    # Duplicates
    # -------------------------------------------------------------------------
    def _validate_duplicates(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        self._validate_duplicate_column(domo_df, inf_df, KEY_UPC_COLUMN, "UPC-006D")
        self._validate_duplicate_column(domo_df, inf_df, KEY_ITEM_COLUMN, "UPC-006E")
        self._validate_duplicate_pair(domo_df, inf_df, "UPC-006F")

    def _validate_duplicate_column(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame, column: str, check_id: str) -> None:
        if column not in domo_df.columns or column not in inf_df.columns:
            self.add_check(
                check_id,
                f"Verify duplicate behavior for {column} is equivalent between Domo and Informatica",
                "FAIL",
                "Population",
                field=column,
                expected=f"{column} exists in both files",
                actual="Missing in one or both files",
            )
            return

        domo_counts = Counter(self._trim_series(domo_df[column]).tolist())
        inf_counts = Counter(self._trim_series(inf_df[column]).tolist())

        rows = []
        all_values = set(domo_counts) | set(inf_counts)
        for value in all_values:
            if domo_counts.get(value, 0) != inf_counts.get(value, 0):
                rows.append(
                    {
                        column: value,
                        "domo_count": domo_counts.get(value, 0),
                        "informatica_count": inf_counts.get(value, 0),
                    }
                )

        if rows:
            evidence_name = f"upc_duplicate_count_differences_{column}"
            self.evidence[evidence_name] = pd.DataFrame(rows[:MAX_EVIDENCE_ROWS])

        self.add_check(
            check_id,
            f"Verify duplicate behavior for {column} is equivalent between Domo and Informatica",
            "PASS" if not rows else "FAIL",
            "Population",
            field=column,
            expected="Same multiplicity per value",
            actual="Matched" if not rows else f"{len(rows)} value(s) have different duplicate counts",
            details="Duplicate multiplicity comparison by value.",
            evidence_file=f"upc_duplicate_count_differences_{column}.csv" if rows else "",
        )

    def _validate_duplicate_pair(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame, check_id: str) -> None:
        pair_cols = [KEY_UPC_COLUMN, KEY_ITEM_COLUMN]
        if any(c not in domo_df.columns for c in pair_cols) or any(c not in inf_df.columns for c in pair_cols):
            self.add_check(
                check_id,
                "Verify duplicate behavior for UPC_Code + Item_Number pairs is equivalent between Domo and Informatica",
                "FAIL",
                "Population",
                field=f"{KEY_UPC_COLUMN}; {KEY_ITEM_COLUMN}",
                expected="Both pair columns exist in both files",
                actual="Missing in one or both files",
            )
            return

        domo_pairs = list(zip(self._trim_series(domo_df[KEY_UPC_COLUMN]), self._trim_series(domo_df[KEY_ITEM_COLUMN])))
        inf_pairs = list(zip(self._trim_series(inf_df[KEY_UPC_COLUMN]), self._trim_series(inf_df[KEY_ITEM_COLUMN])))

        domo_counts = Counter(domo_pairs)
        inf_counts = Counter(inf_pairs)

        rows = []
        all_pairs = set(domo_counts) | set(inf_counts)
        for pair in all_pairs:
            if domo_counts.get(pair, 0) != inf_counts.get(pair, 0):
                rows.append(
                    {
                        KEY_UPC_COLUMN: pair[0],
                        KEY_ITEM_COLUMN: pair[1],
                        "domo_count": domo_counts.get(pair, 0),
                        "informatica_count": inf_counts.get(pair, 0),
                    }
                )

        if rows:
            self.evidence["upc_duplicate_pair_count_differences"] = pd.DataFrame(rows[:MAX_EVIDENCE_ROWS])

        self.add_check(
            check_id,
            "Verify duplicate behavior for UPC_Code + Item_Number pairs is equivalent between Domo and Informatica",
            "PASS" if not rows else "FAIL",
            "Population",
            field=f"{KEY_UPC_COLUMN}; {KEY_ITEM_COLUMN}",
            expected="Same multiplicity per pair",
            actual="Matched" if not rows else f"{len(rows)} pair(s) have different duplicate counts",
            details="Pair multiplicity reconciliation.",
            evidence_file="upc_duplicate_pair_count_differences.csv" if rows else "",
        )

    # -------------------------------------------------------------------------
    # Null / blank
    # -------------------------------------------------------------------------
    def _validate_null_blank_rules(self, inf_df: pd.DataFrame) -> None:
        self._validate_single_null_blank(inf_df, KEY_UPC_COLUMN, "UPC-007A")
        self._validate_single_null_blank(inf_df, KEY_ITEM_COLUMN, "UPC-007B")
        self._validate_single_null_blank(inf_df, KEY_TIMESTAMP_COLUMN, "UPC-007C")

    def _validate_single_null_blank(self, inf_df: pd.DataFrame, column: str, check_id: str) -> None:
        if column not in inf_df.columns:
            self.add_check(
                check_id,
                f"Verify {column} is never null or blank in Informatica output",
                "FAIL",
                "Quality",
                field=column,
                expected=f"{column} exists and is populated",
                actual="Column missing",
            )
            return

        raw = self._safe_series(inf_df, column)
        text = self._normalize_text_series(raw)
        trimmed = text.str.strip()

        null_count = int(raw.isna().sum())
        blank_count = int(trimmed.eq("").sum())
        invalid_text_count = int(trimmed.str.lower().isin(INVALID_TEXT_VALUES).sum())

        observations = []
        if null_count > 0:
            observations.append({"issue": "null", "count": null_count})
        if blank_count > 0:
            observations.append({"issue": "blank_after_trim", "count": blank_count})
        if invalid_text_count > 0:
            observations.append({"issue": "invalid_text_literal", "count": invalid_text_count})

        if observations:
            evidence_name = f"upc_null_blank_{column}"
            self.evidence[evidence_name] = pd.DataFrame(observations)

        self.add_check(
            check_id,
            f"Verify {column} is never null or blank in Informatica output",
            "PASS" if not observations else "FAIL",
            "Quality",
            field=column,
            expected="No nulls, blanks, or invalid text literals",
            actual="No issues found" if not observations else f"{len(observations)} issue type(s) detected",
            details="Strict null / blank / invalid text validation.",
            evidence_file=f"upc_null_blank_{column}.csv" if observations else "",
        )

    # -------------------------------------------------------------------------
    # Text behavior
    # -------------------------------------------------------------------------
    def _validate_text_behavior(self, inf_df: pd.DataFrame) -> None:
        cols = [KEY_UPC_COLUMN, KEY_ITEM_COLUMN, KEY_TIMESTAMP_COLUMN]

        issues = []
        leading_zero_observations = []

        for col in cols:
            if col not in inf_df.columns:
                continue

            raw = self._normalize_text_series(inf_df[col])
            trimmed = raw.str.strip()

            leading_ws = int(raw.str.match(r"^\s+").sum())
            trailing_ws = int(raw.str.match(r".*\s+$").sum())

            if leading_ws > 0 or trailing_ws > 0:
                issues.append(
                    {
                        "column": col,
                        "leading_whitespace_rows": leading_ws,
                        "trailing_whitespace_rows": trailing_ws,
                    }
                )

            if col == KEY_UPC_COLUMN:
                with_leading_zero = int(trimmed.str.match(r"^0+\d+$").fillna(False).sum())
                leading_zero_observations.append(
                    {
                        "column": col,
                        "rows_with_leading_zero_pattern": with_leading_zero,
                    }
                )

        if issues:
            self.evidence["upc_whitespace_issues"] = pd.DataFrame(issues)

        if leading_zero_observations:
            self.evidence["upc_leading_zero_observations"] = pd.DataFrame(leading_zero_observations)

        self.add_check(
            "UPC-007D",
            "Verify whitespace behavior is clean in Informatica UPC output",
            "PASS" if not issues else "FAIL",
            "Quality",
            field="UPC_Code; Item_Number; Timestamp_run",
            expected="No leading or trailing whitespace",
            actual="No issues found" if not issues else f"{len(issues)} column(s) contain whitespace issues",
            details="Strict trim behavior check.",
            evidence_file="upc_whitespace_issues.csv" if issues else "",
        )

        self.add_check(
            "UPC-007E",
            "Verify UPC_Code string behavior preserves text values such as leading zeros",
            "PASS",
            "Quality",
            field=KEY_UPC_COLUMN,
            expected="UPC_Code handled as text without lossy casting",
            actual="Observation generated",
            details="Review upc_leading_zero_observations.csv for informational confirmation.",
            evidence_file="upc_leading_zero_observations.csv",
        )

    # -------------------------------------------------------------------------
    # Timestamp
    # -------------------------------------------------------------------------
    def _validate_timestamp_population_and_parse(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_TIMESTAMP_COLUMN not in inf_df.columns:
            self.add_check(
                "UPC-008A",
                "Verify Timestamp_run is populated and parseable in Informatica output",
                "FAIL",
                "Quality",
                field=KEY_TIMESTAMP_COLUMN,
                expected="Timestamp_run exists and is parseable",
                actual="Column missing",
            )
            self.add_check(
                "UPC-008B",
                "Verify Timestamp_run value range is consistent between Domo and Informatica",
                "FAIL",
                "Quality",
                field=KEY_TIMESTAMP_COLUMN,
                expected="Comparable min/max timestamp window",
                actual="Cannot compare ranges because column is missing in Informatica",
            )
            return

        inf_ts_raw = self._trim_series(inf_df[KEY_TIMESTAMP_COLUMN])
        inf_blank_count = int(inf_ts_raw.eq("").sum())
        inf_parsed = pd.to_datetime(inf_ts_raw.mask(inf_ts_raw.eq(""), pd.NA), errors="coerce")
        inf_invalid_count = int(inf_parsed.isna().sum()) - inf_blank_count

        obs = []
        if inf_blank_count > 0:
            obs.append({"issue": "blank_timestamps", "count": inf_blank_count})
        if inf_invalid_count > 0:
            obs.append({"issue": "invalid_timestamps", "count": inf_invalid_count})

        if obs:
            self.evidence["upc_timestamp_observations"] = pd.DataFrame(obs)

        self.add_check(
            "UPC-008A",
            "Verify Timestamp_run is populated and parseable in Informatica output",
            "PASS" if not obs else "FAIL",
            "Quality",
            field=KEY_TIMESTAMP_COLUMN,
            expected="All rows contain valid parseable timestamps",
            actual="No issues found" if not obs else f"{len(obs)} timestamp issue type(s) detected",
            details="Strict timestamp parse validation over Informatica output.",
            evidence_file="upc_timestamp_observations.csv" if obs else "",
        )

        if KEY_TIMESTAMP_COLUMN not in domo_df.columns:
            self.add_check(
                "UPC-008B",
                "Verify Timestamp_run value range is consistent between Domo and Informatica",
                "FAIL",
                "Quality",
                field=KEY_TIMESTAMP_COLUMN,
                expected="Comparable min/max timestamp window",
                actual="Domo is missing Timestamp_run",
            )
            return

        domo_ts_raw = self._trim_series(domo_df[KEY_TIMESTAMP_COLUMN])
        domo_parsed = pd.to_datetime(domo_ts_raw.mask(domo_ts_raw.eq(""), pd.NA), errors="coerce").dropna()
        inf_valid = inf_parsed.dropna()

        if domo_parsed.empty or inf_valid.empty:
            self.add_check(
                "UPC-008B",
                "Verify Timestamp_run value range is consistent between Domo and Informatica",
                "FAIL",
                "Quality",
                field=KEY_TIMESTAMP_COLUMN,
                expected="Comparable min/max timestamp window",
                actual="One or both sources do not contain valid timestamps",
            )
            return

        domo_min, domo_max = domo_parsed.min(), domo_parsed.max()
        inf_min, inf_max = inf_valid.min(), inf_valid.max()

        range_ok = domo_min == inf_min and domo_max == inf_max

        if not range_ok:
            self.evidence["upc_timestamp_range_difference"] = pd.DataFrame(
                [
                    {
                        "source": "DOMO",
                        "min_timestamp": str(domo_min),
                        "max_timestamp": str(domo_max),
                    },
                    {
                        "source": "INFORMATICA",
                        "min_timestamp": str(inf_min),
                        "max_timestamp": str(inf_max),
                    },
                ]
            )

        self.add_check(
            "UPC-008B",
            "Verify Timestamp_run value range is consistent between Domo and Informatica",
            "PASS" if range_ok else "FAIL",
            "Quality",
            field=KEY_TIMESTAMP_COLUMN,
            expected=f"{domo_min} -> {domo_max}",
            actual=f"{inf_min} -> {inf_max}",
            details="Min/max timestamp window comparison between both files.",
            evidence_file="upc_timestamp_range_difference.csv" if not range_ok else "",
        )

    # -------------------------------------------------------------------------
    # Full reconciliation
    # -------------------------------------------------------------------------
    def _validate_full_row_reconciliation(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if any(col not in domo_df.columns for col in EXPECTED_COLUMNS_BASELINE) or any(
            col not in inf_df.columns for col in EXPECTED_COLUMNS_BASELINE
        ):
            self.add_check(
                "UPC-009",
                "Verify the full normalized UPC row population is equivalent between Domo and Informatica",
                "FAIL",
                "Overall Reconciliation",
                field="All baseline columns",
                expected="All baseline columns exist in both sources",
                actual="Cannot reconcile full rows because one or more baseline columns are missing",
            )
            return

        domo_rows = self._normalized_rows(domo_df)
        inf_rows = self._normalized_rows(inf_df)

        domo_counts = Counter(domo_rows)
        inf_counts = Counter(inf_rows)

        diffs = []
        all_rows = set(domo_counts) | set(inf_counts)
        for row in all_rows:
            if domo_counts.get(row, 0) != inf_counts.get(row, 0):
                diffs.append(
                    {
                        "UPC_Code": row[0],
                        "Item_Number": row[1],
                        "Timestamp_run": row[2],
                        "domo_count": domo_counts.get(row, 0),
                        "informatica_count": inf_counts.get(row, 0),
                    }
                )

        if diffs:
            self.evidence["upc_full_row_differences"] = pd.DataFrame(diffs[:MAX_EVIDENCE_ROWS])

        self.add_check(
            "UPC-009",
            "Verify the full normalized UPC row population is equivalent between Domo and Informatica",
            "PASS" if not diffs else "FAIL",
            "Overall Reconciliation",
            field="UPC_Code; Item_Number; Timestamp_run",
            expected="Exact row-level multiset equivalence after normalization",
            actual="Matched" if not diffs else f"{len(diffs)} normalized row(s) differ in multiplicity",
            details="Strict multiset reconciliation over all baseline columns.",
            evidence_file="upc_full_row_differences.csv" if diffs else "",
        )

    # -------------------------------------------------------------------------
    # Overall
    # -------------------------------------------------------------------------
    def _validate_overall_decision(self) -> None:
        fail_count = sum(1 for c in self.checks if c.status == "FAIL")
        self.add_check(
            "UPC-999",
            "Verify that the full Informatica UPC output is functionally equivalent to Domo",
            "PASS" if fail_count == 0 else "FAIL",
            "Overall Reconciliation",
            field="All records",
            expected="No unresolved variance exists across schema, volume, population, quality, and reconciliation rules",
            actual="Equivalent" if fail_count == 0 else f"{fail_count} blocking failure(s) detected",
        )

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    def _build_summary(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> Dict[str, object]:
        status_counts = Counter(c.status for c in self.checks)

        domo_ts = (
            pd.to_datetime(self._trim_series(domo_df[KEY_TIMESTAMP_COLUMN]).replace("", pd.NA), errors="coerce").dropna()
            if KEY_TIMESTAMP_COLUMN in domo_df.columns
            else pd.Series(dtype="datetime64[ns]")
        )
        inf_ts = (
            pd.to_datetime(self._trim_series(inf_df[KEY_TIMESTAMP_COLUMN]).replace("", pd.NA), errors="coerce").dropna()
            if KEY_TIMESTAMP_COLUMN in inf_df.columns
            else pd.Series(dtype="datetime64[ns]")
        )

        return {
            "dataset": DATASET_NAME,
            "domo_rows": len(domo_df),
            "informatica_rows": len(inf_df),
            "expected_columns": len(EXPECTED_COLUMNS_BASELINE),
            "actual_domo_columns": len(domo_df.columns),
            "actual_informatica_columns": len(inf_df.columns),
            "domo_unique_upc": int(self._trim_series(domo_df[KEY_UPC_COLUMN]).nunique()) if KEY_UPC_COLUMN in domo_df.columns else 0,
            "informatica_unique_upc": int(self._trim_series(inf_df[KEY_UPC_COLUMN]).nunique()) if KEY_UPC_COLUMN in inf_df.columns else 0,
            "domo_unique_item_number": int(self._trim_series(domo_df[KEY_ITEM_COLUMN]).nunique()) if KEY_ITEM_COLUMN in domo_df.columns else 0,
            "informatica_unique_item_number": int(self._trim_series(inf_df[KEY_ITEM_COLUMN]).nunique()) if KEY_ITEM_COLUMN in inf_df.columns else 0,
            "domo_timestamp_min": str(domo_ts.min()) if not domo_ts.empty else "",
            "domo_timestamp_max": str(domo_ts.max()) if not domo_ts.empty else "",
            "informatica_timestamp_min": str(inf_ts.min()) if not inf_ts.empty else "",
            "informatica_timestamp_max": str(inf_ts.max()) if not inf_ts.empty else "",
            "total_checks": len(self.checks),
            "passed": status_counts.get("PASS", 0),
            "failed": status_counts.get("FAIL", 0),
            "warnings": status_counts.get("WARNING", 0),
            "overall_status": "FAIL" if status_counts.get("FAIL", 0) else "PASS",
        }