from __future__ import annotations

from collections import Counter
from typing import Dict, List

import pandas as pd

from products_validator.config.products_config import (
    BOOLEAN_LIKE_FIELD_PREFIXES,
    CODE_FIELD_PREFIXES,
    DATASET_NAME,
    DATE_FIELD_PREFIXES,
    EXPECTED_BASELINE_COLUMN_COUNT,
    EXPECTED_COLUMNS_BASELINE,
    KEY_CHILD_COLUMN,
    KEY_PARENT_COLUMN,
    KEY_PRIMARY_COLUMN,
    MANDATORY_GLOBAL_FIELDS,
    MAX_EVIDENCE_ROWS,
    NUMERIC_FIELD_PREFIXES,
    PRODUCT_COUNTRIES,
    PRODUCT_COUNTRY_COMPARE_FIELDS,
    SNAPSHOT_CANDIDATE_COLUMNS,
    SPECIAL_GLOBAL_FIELDS,
    STRICT_BLANK_ROW_LEVEL_MODE,
    STRICT_SCHEMA_MODE,
    STRICT_SNAPSHOT_MODE,
)
from prices_validator.core.token_parsers import is_blank_like
from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun


class ProductsValidator:
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

        print("  [3/11] Record count validation...")
        self._validate_record_count(domo_df, inf_df)

        print("  [4/11] Key population validation...")
        self._validate_key_population(domo_df, inf_df)

        print("  [5/11] Key string behavior...")
        self._validate_key_string_behavior(domo_df, inf_df)

        print("  [6/11] Blank / whitespace behavior...")
        self._validate_blank_whitespace_behavior(domo_df, inf_df)

        print("  [7/11] Mandatory global fields...")
        self._validate_mandatory_global_fields(domo_df, inf_df)

        print("  [8/11] Global special fields...")
        self._validate_global_special_fields(domo_df, inf_df)

        print("  [9/11] UPC / quick lookup...")
        self._validate_upc_and_quick_lookup(domo_df, inf_df)

        print("  [10/11] Country field validation...")
        self._validate_country_fields(domo_df, inf_df)

        print("  [11/11] Overall decision...")
        self._validate_overall_decision()

        summary = self._build_summary(domo_df, inf_df)
        print("  Validation summary built.")
        return ValidationRun(summary=summary, checks=self.checks, dataframes=self.evidence)

    # -------------------------------------------------------------------------
    # Snapshot
    # -------------------------------------------------------------------------
    def _validate_snapshot(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        manual_domo = self.snapshot_context.domo_snapshot.strip()
        manual_inf = self.snapshot_context.informatica_snapshot.strip()

        if manual_domo or manual_inf:
            status = "PASS" if manual_domo and manual_inf and manual_domo == manual_inf else "FAIL"
            self.add_check(
                "PRODUCTS-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                status,
                "File Control",
                field="Business date, run timestamp, or approved comparison window",
                expected=manual_domo or "Manual snapshot value required",
                actual=manual_inf or "Missing Informatica snapshot value",
                details="Manual snapshot comparison was used.",
            )
            return

        common = [c for c in SNAPSHOT_CANDIDATE_COLUMNS if c in domo_df.columns and c in inf_df.columns]
        if not common:
            self.add_check(
                "PRODUCTS-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date, run timestamp, or approved comparison window",
                expected="Shared metadata columns or manual snapshot inputs",
                actual="No comparable snapshot metadata found in file contents",
                details=(
                    "The files may still be equivalent in content, but snapshot provenance "
                    "could not be verified from file contents alone."
                ),
            )
            return

        mismatches = []
        multiplicity_issues = []

        for col in common:
            domo_vals = sorted(set(domo_df[col].tolist()))
            inf_vals = sorted(set(inf_df[col].tolist()))

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
                        "domo": " | ".join(map(str, domo_vals)),
                        "informatica": " | ".join(map(str, inf_vals)),
                    }
                )

        if multiplicity_issues:
            self.evidence["snapshot_multiplicity_issues"] = pd.DataFrame(multiplicity_issues)

        if mismatches:
            self.evidence["snapshot_mismatches"] = pd.DataFrame(mismatches)
            self.add_check(
                "PRODUCTS-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "FAIL",
                "File Control",
                field="Business date, run timestamp, or approved comparison window",
                expected="Same metadata value set for shared snapshot columns",
                actual=f"{len(mismatches)} mismatching metadata columns",
                details="See snapshot_mismatches.csv",
                evidence_file="snapshot_mismatches.csv",
            )
            return

        if multiplicity_issues:
            self.add_check(
                "PRODUCTS-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date, run timestamp, or approved comparison window",
                expected="Exactly one distinct snapshot value per comparable metadata column in each file",
                actual=f"{len(multiplicity_issues)} metadata columns contain multiple values",
                details=(
                    "Metadata multiplicity detected, but values still match between files. "
                    "See snapshot_multiplicity_issues.csv"
                ),
                evidence_file="snapshot_multiplicity_issues.csv",
            )
            return

        self.add_check(
            "PRODUCTS-001",
            "Verify that Domo and Informatica files belong to the same business snapshot",
            "PASS",
            "File Control",
            field="Business date, run timestamp, or approved comparison window",
            expected="Same metadata value set for shared snapshot columns",
            actual=f"Matched on {len(common)} column(s)",
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
                "PRODUCTS-002A",
                "Verify baseline definition contains the complete expected Products schema",
                "FAIL",
                "Schema",
                field="Baseline configuration",
                expected=f"{EXPECTED_BASELINE_COLUMN_COUNT} columns",
                actual=f"{len(baseline)} columns",
                details="products_config.py EXPECTED_COLUMNS_BASELINE must be corrected before trusting schema checks.",
            )
        else:
            self.add_check(
                "PRODUCTS-002A",
                "Verify baseline definition contains the complete expected Products schema",
                "PASS",
                "Schema",
                field="Baseline configuration",
                expected=f"{EXPECTED_BASELINE_COLUMN_COUNT} columns",
                actual=f"{len(baseline)} columns",
            )

        missing_in_inf = [c for c in baseline if c not in inf_cols]
        extra_in_inf = [c for c in inf_cols if c not in baseline]
        missing_in_domo = [c for c in baseline if c not in domo_cols]
        extra_in_domo = [c for c in domo_cols if c not in baseline]

        exact_order_inf = inf_cols == baseline
        exact_order_domo = domo_cols == baseline

        if missing_in_inf:
            self.evidence["missing_columns_in_informatica"] = pd.DataFrame({"column": missing_in_inf})
        if extra_in_inf:
            self.evidence["extra_columns_in_informatica"] = pd.DataFrame({"column": extra_in_inf})
        if missing_in_domo:
            self.evidence["missing_columns_in_domo"] = pd.DataFrame({"column": missing_in_domo})
        if extra_in_domo:
            self.evidence["extra_columns_in_domo"] = pd.DataFrame({"column": extra_in_domo})

        if not exact_order_inf:
            order_rows = []
            max_len = max(len(baseline), len(inf_cols))
            for idx in range(max_len):
                exp = baseline[idx] if idx < len(baseline) else ""
                act = inf_cols[idx] if idx < len(inf_cols) else ""
                if exp != act:
                    order_rows.append({"position": idx + 1, "expected": exp, "actual": act})
            self.evidence["column_order_differences"] = pd.DataFrame(order_rows)

        self.add_check(
            "PRODUCTS-002",
            "Verify that Informatica preserves the complete Domo schema for the products file",
            "PASS" if not missing_in_inf and not missing_in_domo else "FAIL",
            "Schema",
            field="All columns",
            expected=f"All baseline columns present ({len(baseline)})",
            actual=f"Missing in Domo: {len(missing_in_domo)}, Missing in Informatica: {len(missing_in_inf)}",
            details=(
                f"Domo missing: {', '.join(missing_in_domo[:10])} | "
                f"Informatica missing: {', '.join(missing_in_inf[:10])}"
            ).strip(" |")
            if (missing_in_domo or missing_in_inf)
            else "No missing columns",
            evidence_file="missing_columns_in_informatica.csv" if missing_in_inf else ("missing_columns_in_domo.csv" if missing_in_domo else ""),
        )

        self.add_check(
            "PRODUCTS-003",
            "Verify that Informatica does not introduce unexpected columns or alter header naming",
            "PASS" if not extra_in_inf and not extra_in_domo and set(inf_cols) == set(baseline) and set(domo_cols) == set(baseline) else "FAIL",
            "Schema",
            field="All columns",
            expected="No extras + exact header text",
            actual=f"Extra in Domo: {len(extra_in_domo)}, Extra in Informatica: {len(extra_in_inf)}",
            details=(
                f"Domo extras: {', '.join(extra_in_domo[:10])} | "
                f"Informatica extras: {', '.join(extra_in_inf[:10])}"
            ).strip(" |")
            if (extra_in_domo or extra_in_inf)
            else "No unexpected columns",
            evidence_file="extra_columns_in_informatica.csv" if extra_in_inf else ("extra_columns_in_domo.csv" if extra_in_domo else ""),
        )

        self.add_check(
            "PRODUCTS-004",
            "Verify that column order in Informatica matches the Domo baseline",
            "PASS" if exact_order_inf and exact_order_domo else "FAIL",
            "Schema",
            field="All columns",
            expected="Exact header sequence",
            actual=(
                "Matched"
                if exact_order_inf and exact_order_domo
                else f"Domo order ok={exact_order_domo}, Informatica order ok={exact_order_inf}"
            ),
            details="See column_order_differences.csv" if not exact_order_inf else "",
            evidence_file="column_order_differences.csv" if not exact_order_inf else "",
        )

    # -------------------------------------------------------------------------
    # Record count
    # -------------------------------------------------------------------------
    def _validate_record_count(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        status = "PASS" if len(domo_df) == len(inf_df) else "FAIL"
        self.add_check(
            "PRODUCTS-005",
            "Verify that the total record count in Informatica matches Domo",
            status,
            "Volume",
            field="All records",
            expected=str(len(domo_df)),
            actual=str(len(inf_df)),
        )

    # -------------------------------------------------------------------------
    # Key population
    # -------------------------------------------------------------------------
    def _validate_key_population(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        for idx, key_col in enumerate([KEY_PRIMARY_COLUMN, KEY_PARENT_COLUMN, KEY_CHILD_COLUMN], start=1):
            check_id = f"PRODUCTS-006{chr(96 + idx)}"

            if key_col not in domo_df.columns or key_col not in inf_df.columns:
                self.add_check(
                    check_id,
                    f"Verify that the {key_col} population matches exactly between Domo and Informatica",
                    "FAIL",
                    "Population",
                    field=key_col,
                    expected=f"Column {key_col} exists in both files",
                    actual="Missing in one or both files",
                )
                continue

            domo_vals = domo_df[key_col].tolist()
            inf_vals = inf_df[key_col].tolist()
            domo_set = set(domo_vals)
            inf_set = set(inf_vals)

            only_domo = sorted(domo_set - inf_set)
            only_inf = sorted(inf_set - domo_set)

            if only_domo:
                self.evidence[f"{key_col}_only_in_domo"] = pd.DataFrame({key_col: only_domo})
            if only_inf:
                self.evidence[f"{key_col}_only_in_informatica"] = pd.DataFrame({key_col: only_inf})

            domo_dupes = self._duplicate_counts(domo_vals)
            inf_dupes = self._duplicate_counts(inf_vals)
            dupes_diff = self._diff_duplicate_behavior(domo_dupes, inf_dupes, key_col)

            if dupes_diff:
                self.evidence[f"{key_col}_duplicate_behavior_differences"] = pd.DataFrame(dupes_diff)

            status = "PASS" if not only_domo and not only_inf and not dupes_diff else "FAIL"
            self.add_check(
                check_id,
                f"Verify that the {key_col} population matches exactly between Domo and Informatica",
                status,
                "Population",
                field=key_col,
                expected="Set equality + duplicate behavior",
                actual=(
                    f"only_in_domo={len(only_domo)}, only_in_informatica={len(only_inf)}, "
                    f"duplicate_behavior_differences={len(dupes_diff)}"
                ),
                details="See evidence files if generated.",
            )

    # -------------------------------------------------------------------------
    # Key string behavior
    # -------------------------------------------------------------------------
    def _validate_key_string_behavior(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        issues: List[Dict[str, str]] = []

        for key_col in [KEY_PRIMARY_COLUMN, KEY_PARENT_COLUMN, KEY_CHILD_COLUMN]:
            if key_col not in domo_df.columns or key_col not in inf_df.columns:
                continue

            common = sorted(set(domo_df[key_col].tolist()) & set(inf_df[key_col].tolist()))
            inf_set = set(inf_df[key_col].tolist())

            for value in common:
                if not isinstance(value, str):
                    issues.append({"field": key_col, "value": str(value), "issue": "Non-string value detected"})
                    continue

                if "e+" in value.lower() or "e-" in value.lower():
                    issues.append({"field": key_col, "value": value, "issue": "Scientific notation detected"})

                if value != value.strip():
                    issues.append({"field": key_col, "value": value, "issue": "Leading or trailing whitespace detected"})

                if value.isdigit() and len(value) > 1 and value.startswith("0") and value not in inf_set:
                    issues.append({"field": key_col, "value": value, "issue": "Leading zero loss suspected"})

        status = "WARNING" if issues else "PASS"
        if issues:
            self.evidence["key_string_issues"] = pd.DataFrame(issues)

        self.add_check(
            "PRODUCTS-007",
            "Verify key fields preserve string behavior and original formatting in Informatica",
            status,
            "Datatype",
            field=f"{KEY_PRIMARY_COLUMN}; {KEY_PARENT_COLUMN}; {KEY_CHILD_COLUMN}",
            expected="String comparison; no cast; no format loss",
            actual="No formatting issue detected" if status == "PASS" else f"{len(issues)} format observations",
            details="In compare mode, these observations do not fail equivalence. See key_string_issues.csv if generated.",
            evidence_file="key_string_issues.csv" if issues else "",
        )

    # -------------------------------------------------------------------------
    # Blank / whitespace
    # -------------------------------------------------------------------------
    def _validate_blank_whitespace_behavior(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        common_cols = [c for c in domo_df.columns if c in inf_df.columns]
        count_results = []
        row_level_results = []

        columns_with_count_differences = []

        for col in common_cols:
            domo_series = domo_df[col]
            inf_series = inf_df[col]

            domo_blank = int(domo_series.apply(is_blank_like).sum())
            inf_blank = int(inf_series.apply(is_blank_like).sum())

            domo_ws = int(domo_series.apply(lambda x: x != x.strip() if isinstance(x, str) else False).sum())
            inf_ws = int(inf_series.apply(lambda x: x != x.strip() if isinstance(x, str) else False).sum())

            if domo_blank != inf_blank or domo_ws != inf_ws:
                columns_with_count_differences.append(col)
                count_results.append(
                    {
                        "column": col,
                        "domo_blank_count": domo_blank,
                        "informatica_blank_count": inf_blank,
                        "domo_whitespace_variants": domo_ws,
                        "informatica_whitespace_variants": inf_ws,
                    }
                )

        if (
            STRICT_BLANK_ROW_LEVEL_MODE
            and columns_with_count_differences
            and KEY_PRIMARY_COLUMN in domo_df.columns
            and KEY_PRIMARY_COLUMN in inf_df.columns
        ):
            domo_unique = domo_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()
            inf_unique = inf_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()

            domo_indexed = domo_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)
            inf_indexed = inf_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)

            common_keys = sorted(set(domo_indexed.index) & set(inf_indexed.index))

            for col in columns_with_count_differences:
                for key in common_keys:
                    domo_val = domo_indexed.at[key, col]
                    inf_val = inf_indexed.at[key, col]

                    domo_blank_like = is_blank_like(domo_val)
                    inf_blank_like = is_blank_like(inf_val)

                    domo_ws = domo_val != domo_val.strip() if isinstance(domo_val, str) else False
                    inf_ws = inf_val != inf_val.strip() if isinstance(inf_val, str) else False

                    if domo_blank_like != inf_blank_like or domo_ws != inf_ws:
                        if len(row_level_results) < MAX_EVIDENCE_ROWS:
                            row_level_results.append(
                                {
                                    KEY_PRIMARY_COLUMN: key,
                                    "column": col,
                                    "domo_value": domo_val,
                                    "informatica_value": inf_val,
                                    "domo_blank_like": domo_blank_like,
                                    "informatica_blank_like": inf_blank_like,
                                    "domo_whitespace_variant": domo_ws,
                                    "informatica_whitespace_variant": inf_ws,
                                }
                            )

        if count_results:
            self.evidence["blank_whitespace_differences"] = pd.DataFrame(count_results)
        if row_level_results:
            self.evidence["blank_whitespace_row_level_differences"] = pd.DataFrame(row_level_results)

        self.add_check(
            "PRODUCTS-008",
            "Verify that blank, null-like, and whitespace behavior is preserved exactly between Domo and Informatica",
            "PASS" if not count_results and not row_level_results else "FAIL",
            "Null Handling",
            field="All columns",
            expected="Blank distribution + whitespace preservation",
            actual="Matched" if not count_results and not row_level_results else f"{len(count_results)} columns with differences",
            details=(
                "See blank_whitespace_row_level_differences.csv if generated."
                if row_level_results
                else "See blank_whitespace_differences.csv if generated."
            ),
            evidence_file=(
                "blank_whitespace_row_level_differences.csv"
                if row_level_results
                else ("blank_whitespace_differences.csv" if count_results else "")
            ),
        )

    # -------------------------------------------------------------------------
    # Mandatory global fields
    # -------------------------------------------------------------------------
    def _validate_mandatory_global_fields(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_PRIMARY_COLUMN not in domo_df.columns or KEY_PRIMARY_COLUMN not in inf_df.columns:
            return

        domo_unique = domo_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()
        inf_unique = inf_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()

        domo_indexed = domo_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)
        inf_indexed = inf_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)

        common_keys = sorted(set(domo_indexed.index) & set(inf_indexed.index))

        diffs = []
        internal_null_observations = []

        for field_name in MANDATORY_GLOBAL_FIELDS:
            if field_name not in domo_df.columns or field_name not in inf_df.columns:
                self.add_check(
                    f"PRODUCTS-MAND-{field_name}",
                    f"Verify mandatory field {field_name} exists in both files",
                    "FAIL",
                    "Mandatory Fields",
                    field=field_name,
                    expected="Field exists in both files",
                    actual="Missing in one or both files",
                )
                continue

            mismatch_count = 0
            null_observation_count = 0

            for key in common_keys:
                domo_val = domo_indexed.at[key, field_name]
                inf_val = inf_indexed.at[key, field_name]

                if domo_val != inf_val:
                    mismatch_count += 1
                    if len(diffs) < MAX_EVIDENCE_ROWS:
                        diffs.append(
                            {
                                KEY_PRIMARY_COLUMN: key,
                                "field": field_name,
                                "domo": domo_val,
                                "informatica": inf_val,
                            }
                        )

                if is_blank_like(domo_val) or is_blank_like(inf_val):
                    null_observation_count += 1
                    if len(internal_null_observations) < MAX_EVIDENCE_ROWS:
                        internal_null_observations.append(
                            {
                                KEY_PRIMARY_COLUMN: key,
                                "field": field_name,
                                "domo": domo_val,
                                "informatica": inf_val,
                            }
                        )

            self.add_check(
                f"PRODUCTS-MAND-{field_name}",
                f"Verify mandatory field {field_name} is preserved exactly in Informatica",
                "PASS" if mismatch_count == 0 else "FAIL",
                "Mandatory Fields",
                field=field_name,
                expected="Exact raw equality and mandatory field preservation",
                actual="Matched" if mismatch_count == 0 else f"{mismatch_count} raw differences",
                details=(
                    f"{null_observation_count} null/blank observation(s) detected, but in compare mode "
                    "only raw differences block equivalence."
                    if null_observation_count > 0
                    else ""
                ),
                evidence_file="mandatory_global_field_differences.csv" if mismatch_count > 0 else ("mandatory_global_field_null_observations.csv" if null_observation_count > 0 else ""),
            )

        if diffs:
            self.evidence["mandatory_global_field_differences"] = pd.DataFrame(diffs)
        if internal_null_observations:
            self.evidence["mandatory_global_field_null_observations"] = pd.DataFrame(internal_null_observations)

    # -------------------------------------------------------------------------
    # Global special fields
    # -------------------------------------------------------------------------
    def _validate_global_special_fields(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_PRIMARY_COLUMN not in domo_df.columns or KEY_PRIMARY_COLUMN not in inf_df.columns:
            return

        domo_unique = domo_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()
        inf_unique = inf_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()

        domo_indexed = domo_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)
        inf_indexed = inf_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)

        common_keys = sorted(set(domo_indexed.index) & set(inf_indexed.index))

        field_diff_records = []
        observations = []

        for field_name in SPECIAL_GLOBAL_FIELDS:
            if field_name not in domo_df.columns or field_name not in inf_df.columns:
                continue

            mismatch_count = 0
            observation_count = 0

            for key in common_keys:
                domo_val = domo_indexed.at[key, field_name]
                inf_val = inf_indexed.at[key, field_name]

                if domo_val != inf_val:
                    mismatch_count += 1
                    if len(field_diff_records) < MAX_EVIDENCE_ROWS:
                        field_diff_records.append(
                            {
                                KEY_PRIMARY_COLUMN: key,
                                "field": field_name,
                                "domo": domo_val,
                                "informatica": inf_val,
                            }
                        )

                if self._is_boolean_like_field(field_name) and not self._is_boolean_like_value(inf_val):
                    observation_count += 1
                    if len(observations) < MAX_EVIDENCE_ROWS:
                        observations.append(
                            {
                                KEY_PRIMARY_COLUMN: key,
                                "field": field_name,
                                "value": inf_val,
                                "issue": "Boolean-like format observation",
                            }
                        )

            self.add_check(
                f"PRODUCTS-SPECIAL-{field_name}",
                f"Verify special global field {field_name} is preserved exactly in Informatica",
                "PASS" if mismatch_count == 0 else "FAIL",
                "Global Content",
                field=field_name,
                expected="Exact raw equality",
                actual="Matched" if mismatch_count == 0 else f"{mismatch_count} raw differences",
                details=(
                    f"{observation_count} format observation(s) detected, but in compare mode "
                    "only raw differences block equivalence."
                    if observation_count > 0
                    else ""
                ),
                evidence_file="special_global_field_differences.csv" if mismatch_count > 0 else ("special_global_field_observations.csv" if observation_count > 0 else ""),
            )

        if field_diff_records:
            self.evidence["special_global_field_differences"] = pd.DataFrame(field_diff_records)
        if observations:
            self.evidence["special_global_field_observations"] = pd.DataFrame(observations)

    # -------------------------------------------------------------------------
    # UPC / quick lookup
    # -------------------------------------------------------------------------
    def _validate_upc_and_quick_lookup(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        fields = [
            "upc",
            "upc_2",
            "upc_3",
            "QUICK_LOOKUP_PLU_CODE",
            "QUICK_LOOKUP_PACKAGE_NAME",
            "QUICK_LOOKUP_CATEGORY_DISPLAY_NAME",
            "QUICK_LOOKUP_CATEGORY_KEY",
        ]

        if KEY_PRIMARY_COLUMN not in domo_df.columns or KEY_PRIMARY_COLUMN not in inf_df.columns:
            return

        domo_unique = domo_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()
        inf_unique = inf_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()

        domo_indexed = domo_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)
        inf_indexed = inf_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)

        common_keys = sorted(set(domo_indexed.index) & set(inf_indexed.index))

        diffs = []

        for field_name in fields:
            if field_name not in domo_df.columns or field_name not in inf_df.columns:
                continue

            mismatch_count = 0
            for key in common_keys:
                domo_val = domo_indexed.at[key, field_name]
                inf_val = inf_indexed.at[key, field_name]

                if domo_val != inf_val:
                    mismatch_count += 1
                    if len(diffs) < MAX_EVIDENCE_ROWS:
                        diffs.append(
                            {
                                KEY_PRIMARY_COLUMN: key,
                                "field": field_name,
                                "domo": domo_val,
                                "informatica": inf_val,
                            }
                        )

            self.add_check(
                f"PRODUCTS-LINK-{field_name}",
                f"Verify field {field_name} is preserved exactly in Informatica",
                "PASS" if mismatch_count == 0 else "FAIL",
                "Linking / UPC / Quick Lookup",
                field=field_name,
                expected="Exact raw equality",
                actual="Matched" if mismatch_count == 0 else f"{mismatch_count} raw differences",
                evidence_file="upc_quick_lookup_differences.csv" if mismatch_count > 0 else "",
            )

        if diffs:
            self.evidence["upc_quick_lookup_differences"] = pd.DataFrame(diffs)

    # -------------------------------------------------------------------------
    # Country fields
    # -------------------------------------------------------------------------
    def _validate_country_fields(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_PRIMARY_COLUMN not in domo_df.columns or KEY_PRIMARY_COLUMN not in inf_df.columns:
            return

        print("    Preparing indexed data for country validation...")

        domo_unique = domo_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()
        inf_unique = inf_df.drop_duplicates(subset=[KEY_PRIMARY_COLUMN], keep="first").copy()

        domo_indexed = domo_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)
        inf_indexed = inf_unique.set_index(KEY_PRIMARY_COLUMN, drop=False)

        common_keys = sorted(set(domo_indexed.index) & set(inf_indexed.index))
        print(f"    Common unique keys for country checks: {len(common_keys):,}")

        domo_rows = domo_indexed.to_dict(orient="index")
        inf_rows = inf_indexed.to_dict(orient="index")

        field_diff_records: List[Dict[str, str]] = []
        format_observations: List[Dict[str, str]] = []
        cross_field_observations: List[Dict[str, str]] = []

        for idx, country in enumerate(PRODUCT_COUNTRIES, start=1):
            print(f"    [{idx}/{len(PRODUCT_COUNTRIES)}] Country {country}...")

            fields = self._country_fields_for_country(country)
            if not fields:
                continue

            for field_name in fields:
                mismatch_count = 0
                observation_count = 0

                for key in common_keys:
                    row_a = domo_rows[key]
                    row_b = inf_rows[key]

                    domo_val = row_a[field_name]
                    inf_val = row_b[field_name]

                    if domo_val != inf_val:
                        mismatch_count += 1
                        if len(field_diff_records) < MAX_EVIDENCE_ROWS:
                            field_diff_records.append(
                                {
                                    KEY_PRIMARY_COLUMN: key,
                                    "country": country,
                                    "field": field_name,
                                    "domo": domo_val,
                                    "informatica": inf_val,
                                }
                            )

                    issues = self._field_format_observations(field_name, inf_val)
                    if issues:
                        observation_count += 1
                        if len(format_observations) < MAX_EVIDENCE_ROWS:
                            format_observations.append(
                                {
                                    KEY_PRIMARY_COLUMN: key,
                                    "country": country,
                                    "field": field_name,
                                    "value": inf_val,
                                    "issue": " | ".join(issues),
                                }
                            )

                self.add_check(
                    f"PRODUCTS-{country}-{field_name}",
                    f"Verify that {field_name} is preserved exactly in Informatica",
                    "PASS" if mismatch_count == 0 else "FAIL",
                    "Country Content",
                    country=country,
                    field=field_name,
                    expected="Exact raw equality",
                    actual="Matched" if mismatch_count == 0 else f"{mismatch_count} raw differences",
                    details=(
                        f"{observation_count} format observation(s) detected, but in compare mode "
                        "only raw differences block equivalence."
                        if observation_count > 0
                        else ""
                    ),
                    evidence_file="products_country_field_differences.csv" if mismatch_count > 0 else ("products_country_field_observations.csv" if observation_count > 0 else ""),
                )

            self._validate_country_cross_field_alignment(
                country=country,
                common_keys=common_keys,
                domo_rows=domo_rows,
                inf_rows=inf_rows,
                cross_field_observations=cross_field_observations,
            )

        if field_diff_records:
            self.evidence["products_country_field_differences"] = pd.DataFrame(field_diff_records)
        if format_observations:
            self.evidence["products_country_field_observations"] = pd.DataFrame(format_observations)
        if cross_field_observations:
            self.evidence["products_country_cross_field_observations"] = pd.DataFrame(cross_field_observations)

    def _validate_country_cross_field_alignment(
        self,
        country: str,
        common_keys: List[str],
        domo_rows: Dict[str, Dict[str, object]],
        inf_rows: Dict[str, Dict[str, object]],
        cross_field_observations: List[Dict[str, str]],
    ) -> None:
        candidate_fields = [
            self._country_column("Weight", country),
            self._country_column("Unit_Price", country),
            self._country_column("Price_Per_UOM", country),
            self._country_column("POS_Status", country),
            self._country_column("Availability", country),
            self._country_column("POS_UOM", country),
            self._country_column_lc("tax_flag", country),
            self._country_column_lc("tax_plan", country),
            self._country_column_lc("reason_code", country),
            self._country_column_lc("eligible_accrue_platinum", country),
            self._country_column_lc("is_deposit", country),
            self._country_column_lc("linked_item", country),
        ]

        candidate_fields = [
            f for f in candidate_fields
            if f in EXPECTED_COLUMNS_BASELINE
        ]

        if not candidate_fields:
            return

        mismatch_count = 0
        internal_observations = 0

        for key in common_keys:
            domo_present = [f for f in candidate_fields if not is_blank_like(domo_rows[key].get(f, ""))]
            inf_present = [f for f in candidate_fields if not is_blank_like(inf_rows[key].get(f, ""))]

            if domo_present != inf_present:
                mismatch_count += 1
                if len(cross_field_observations) < MAX_EVIDENCE_ROWS:
                    cross_field_observations.append(
                        {
                            KEY_PRIMARY_COLUMN: key,
                            "country": country,
                            "issue_type": "between_file_presence_difference",
                            "domo_present_fields": " | ".join(domo_present),
                            "informatica_present_fields": " | ".join(inf_present),
                        }
                    )
            else:
                # Observación interna: linked_item populated but deposit empty, etc.
                linked_item_field = self._country_column_lc("linked_item", country)
                is_deposit_field = self._country_column_lc("is_deposit", country)
                reason_code_field = self._country_column_lc("reason_code", country)
                tax_plan_field = self._country_column_lc("tax_plan", country)

                if linked_item_field in inf_rows[key]:
                    linked_val = inf_rows[key].get(linked_item_field, "")
                    deposit_val = inf_rows[key].get(is_deposit_field, "") if is_deposit_field in inf_rows[key] else ""
                    reason_val = inf_rows[key].get(reason_code_field, "") if reason_code_field in inf_rows[key] else ""
                    tax_plan_val = inf_rows[key].get(tax_plan_field, "") if tax_plan_field in inf_rows[key] else ""

                    if not is_blank_like(linked_val) and is_blank_like(deposit_val):
                        internal_observations += 1
                        if len(cross_field_observations) < MAX_EVIDENCE_ROWS:
                            cross_field_observations.append(
                                {
                                    KEY_PRIMARY_COLUMN: key,
                                    "country": country,
                                    "issue_type": "internal_consistency_observation",
                                    "domo_present_fields": "",
                                    "informatica_present_fields": (
                                        f"linked_item={linked_val} | is_deposit={deposit_val} | "
                                        f"reason_code={reason_val} | tax_plan={tax_plan_val}"
                                    ),
                                }
                            )

        self.add_check(
            f"PRODUCTS-{country}-XFIELD",
            f"Verify that country-level cross-field consistency is preserved for {country}",
            "PASS" if mismatch_count == 0 else "FAIL",
            "Cross-Field",
            country=country,
            field="country-level related fields",
            expected="Same cross-field field-presence behavior between Domo and Informatica",
            actual="Matched" if mismatch_count == 0 else f"{mismatch_count} keys with between-file cross-field differences",
            details=(
                f"{internal_observations} internal observation(s) detected, but in compare mode these do not fail equivalence."
                if internal_observations > 0
                else ""
            ),
            evidence_file="products_country_cross_field_observations.csv" if (mismatch_count > 0 or internal_observations > 0) else "",
        )

    # -------------------------------------------------------------------------
    # Overall
    # -------------------------------------------------------------------------
    def _validate_overall_decision(self) -> None:
        blocking_statuses = [c.status for c in self.checks if c.status == "FAIL"]
        self.add_check(
            "PRODUCTS-999",
            "Verify that the full Informatica products output is functionally equivalent to Domo across all countries",
            "PASS" if not blocking_statuses else "FAIL",
            "Overall Reconciliation",
            field="All countries",
            expected="No unresolved variance exists across the product-level and country-level validation scope; Informatica is functionally equivalent to Domo for the products dataset.",
            actual="Equivalent" if not blocking_statuses else f"{len(blocking_statuses)} blocking failures detected",
        )

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    def _build_summary(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> Dict[str, object]:
        status_counts = Counter(c.status for c in self.checks)
        return {
            "dataset": DATASET_NAME,
            "domo_rows": len(domo_df),
            "informatica_rows": len(inf_df),
            "expected_columns": len(EXPECTED_COLUMNS_BASELINE),
            "actual_domo_columns": len(domo_df.columns),
            "actual_informatica_columns": len(inf_df.columns),
            "total_checks": len(self.checks),
            "passed": status_counts.get("PASS", 0),
            "failed": status_counts.get("FAIL", 0),
            "warnings": status_counts.get("WARNING", 0),
            "overall_status": "FAIL" if status_counts.get("FAIL", 0) else "PASS",
        }

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _duplicate_counts(values: List[str]) -> Dict[str, int]:
        counter = Counter(values)
        return {key: count for key, count in counter.items() if count > 1}

    @staticmethod
    def _diff_duplicate_behavior(domo_dupes: Dict[str, int], inf_dupes: Dict[str, int], key_col: str) -> List[Dict[str, object]]:
        rows = []
        for key in sorted(set(domo_dupes.keys()) | set(inf_dupes.keys())):
            domo_count = domo_dupes.get(key, 0)
            inf_count = inf_dupes.get(key, 0)
            if domo_count != inf_count:
                rows.append(
                    {
                        key_col: key,
                        "domo_duplicate_count": domo_count,
                        "informatica_duplicate_count": inf_count,
                    }
                )
        return rows

    @staticmethod
    def _country_column(prefix: str, country: str) -> str:
        return f"{prefix}_{country}"

    @staticmethod
    def _country_column_lc(prefix: str, country: str) -> str:
        return f"{prefix}_{country.lower()}"

    def _country_fields_for_country(self, country: str) -> List[str]:
        fields = []
        for template in PRODUCT_COUNTRY_COMPARE_FIELDS:
            field_name = template.format(country=country, country_lc=country.lower())
            if field_name in EXPECTED_COLUMNS_BASELINE:
                fields.append(field_name)
        return fields

    @staticmethod
    def _matches_prefix(field_name: str, prefixes: List[str]) -> bool:
        return any(field_name.startswith(prefix) for prefix in prefixes)

    def _field_format_observations(self, field_name: str, value: object) -> List[str]:
        issues: List[str] = []

        if is_blank_like(value):
            return issues

        if self._matches_prefix(field_name, NUMERIC_FIELD_PREFIXES):
            if not self._is_numeric_like(value):
                issues.append("Numeric-like format observation")

        if self._matches_prefix(field_name, DATE_FIELD_PREFIXES):
            if not self._is_date_like(value):
                issues.append("Date-like format observation")

        if self._matches_prefix(field_name, BOOLEAN_LIKE_FIELD_PREFIXES):
            if not self._is_boolean_like_value(value):
                issues.append("Boolean-like format observation")

        if self._matches_prefix(field_name, CODE_FIELD_PREFIXES):
            if isinstance(value, str) and value != value.strip():
                issues.append("Code contains leading/trailing whitespace")

        return issues

    @staticmethod
    def _is_numeric_like(value: object) -> bool:
        if is_blank_like(value):
            return True
        try:
            float(str(value).replace(",", "").strip())
            return True
        except Exception:
            return False

    @staticmethod
    def _is_date_like(value: object) -> bool:
        if is_blank_like(value):
            return True
        parsed = pd.to_datetime(str(value), errors="coerce")
        return pd.notna(parsed)

    @staticmethod
    def _is_boolean_like_field(field_name: str) -> bool:
        return any(
            field_name == prefix or field_name.startswith(prefix)
            for prefix in BOOLEAN_LIKE_FIELD_PREFIXES
        )

    @staticmethod
    def _is_boolean_like_value(value: object) -> bool:
        if is_blank_like(value):
            return True

        normalized = str(value).strip().lower()
        return normalized in {
            "0", "1",
            "true", "false",
            "yes", "no",
            "y", "n",
            "t", "f",
        }