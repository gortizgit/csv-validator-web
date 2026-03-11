from __future__ import annotations

from collections import Counter
from typing import Dict, List

import pandas as pd

from prices_validator.config.prices_config import (
    CODE_FIELD_PREFIXES,
    COUNTRY_COMPARE_FIELDS,
    COUNTRY_FIELD_TEMPLATES,
    DATASET_NAME,
    EXPECTED_BASELINE_COLUMN_COUNT,
    EXPECTED_COLUMNS_BASELINE,
    KEY_COLUMN,
    MAX_EVIDENCE_ROWS,
    PRICE_COUNTRIES,
    SNAPSHOT_CANDIDATE_COLUMNS,
    STRICT_BLANK_ROW_LEVEL_MODE,
    STRICT_SCHEMA_MODE,
    STRICT_SNAPSHOT_MODE,
    STRICT_STRUCTURE_BOTH_SIDES_MODE,
)
from prices_validator.core.token_parsers import (
    extract_store_sequence_from_cost_center,
    extract_store_sequence_from_structured,
    is_blank_like,
    parse_cost_center,
    parse_country_store_metric,
    validate_code_value,
)
from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun


class PricesValidator:
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
        print("  [1/8] Snapshot validation...")
        self._validate_snapshot(domo_df, inf_df)

        print("  [2/8] Schema validation...")
        self._validate_schema(domo_df, inf_df)

        print("  [3/8] Record count validation...")
        self._validate_record_count(domo_df, inf_df)

        print("  [4/8] Parent_Item_Code population...")
        self._validate_parent_item_population(domo_df, inf_df)

        print("  [5/8] Parent_Item_Code string behavior...")
        self._validate_parent_item_string_behavior(domo_df, inf_df)

        print("  [6/8] Blank / whitespace behavior...")
        self._validate_blank_whitespace_behavior(domo_df, inf_df)

        print("  [7/8] Country field validation...")
        self._validate_country_fields(domo_df, inf_df)

        print("  [8/8] Overall decision...")
        self._validate_overall_decision()

        summary = self._build_summary(domo_df, inf_df)
        print("  Validation summary built.")
        return ValidationRun(summary=summary, checks=self.checks, dataframes=self.evidence)

    def _validate_snapshot(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        manual_domo = self.snapshot_context.domo_snapshot.strip()
        manual_inf = self.snapshot_context.informatica_snapshot.strip()

        if manual_domo or manual_inf:
            status = "PASS" if manual_domo and manual_inf and manual_domo == manual_inf else "FAIL"
            self.add_check(
                "PRICES-001",
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
                "PRICES-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date, run timestamp, or approved comparison window",
                expected="Shared metadata columns or manual snapshot inputs",
                actual="No comparable snapshot metadata found in file contents",
                details="Provide snapshot values in the UI/CLI if the dataset does not carry business snapshot fields.",
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
                "PRICES-001",
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
                "PRICES-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date, run timestamp, or approved comparison window",
                expected="Exactly one distinct snapshot value per comparable metadata column in each file",
                actual=f"{len(multiplicity_issues)} metadata columns contain multiple values",
                details="Metadata multiplicity detected, but values still match between files. See snapshot_multiplicity_issues.csv",
                evidence_file="snapshot_multiplicity_issues.csv",
            )
            return

        self.add_check(
            "PRICES-001",
            "Verify that Domo and Informatica files belong to the same business snapshot",
            "PASS",
            "File Control",
            field="Business date, run timestamp, or approved comparison window",
            expected="Same metadata value set for shared snapshot columns",
            actual=f"Matched on {len(common)} column(s)",
        )

    def _validate_schema(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        domo_cols = list(domo_df.columns)
        inf_cols = list(inf_df.columns)
        baseline = EXPECTED_COLUMNS_BASELINE

        if STRICT_SCHEMA_MODE and len(baseline) != EXPECTED_BASELINE_COLUMN_COUNT:
            self.add_check(
                "PRICES-002A",
                "Verify baseline definition contains the complete expected Prices schema",
                "FAIL",
                "Schema",
                field="Baseline configuration",
                expected=f"{EXPECTED_BASELINE_COLUMN_COUNT} columns",
                actual=f"{len(baseline)} columns",
                details="prices_config.py EXPECTED_COLUMNS_BASELINE must be corrected before trusting schema checks.",
            )
        else:
            self.add_check(
                "PRICES-002A",
                "Verify baseline definition contains the complete expected Prices schema",
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
            "PRICES-002",
            "Verify that Informatica preserves the complete Domo schema for the prices file",
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
            "PRICES-003",
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
            "PRICES-004",
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

    def _validate_record_count(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        status = "PASS" if len(domo_df) == len(inf_df) else "FAIL"
        self.add_check(
            "PRICES-005",
            "Verify that the total record count in Informatica matches Domo",
            status,
            "Volume",
            field="All records",
            expected=str(len(domo_df)),
            actual=str(len(inf_df)),
        )

    def _validate_parent_item_population(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_COLUMN not in domo_df.columns or KEY_COLUMN not in inf_df.columns:
            self.add_check(
                "PRICES-006",
                "Verify that the Parent_Item_Code population matches exactly between Domo and Informatica",
                "FAIL",
                "Population",
                field=KEY_COLUMN,
                expected=f"Column {KEY_COLUMN} exists in both files",
                actual="Missing in one or both files",
            )
            return

        domo_vals = domo_df[KEY_COLUMN].tolist()
        inf_vals = inf_df[KEY_COLUMN].tolist()
        domo_set = set(domo_vals)
        inf_set = set(inf_vals)

        only_domo = sorted(domo_set - inf_set)
        only_inf = sorted(inf_set - domo_set)

        if only_domo:
            self.evidence["parent_item_only_in_domo"] = pd.DataFrame({KEY_COLUMN: only_domo})
        if only_inf:
            self.evidence["parent_item_only_in_informatica"] = pd.DataFrame({KEY_COLUMN: only_inf})

        domo_dupes = self._duplicate_counts(domo_vals)
        inf_dupes = self._duplicate_counts(inf_vals)
        dupes_diff = self._diff_duplicate_behavior(domo_dupes, inf_dupes)

        if dupes_diff:
            self.evidence["duplicate_behavior_differences"] = pd.DataFrame(dupes_diff)

        status = "PASS" if not only_domo and not only_inf and not dupes_diff else "FAIL"
        self.add_check(
            "PRICES-006",
            "Verify that the Parent_Item_Code population matches exactly between Domo and Informatica",
            status,
            "Population",
            field=KEY_COLUMN,
            expected="Set equality + duplicate behavior",
            actual=(
                f"only_in_domo={len(only_domo)}, only_in_informatica={len(only_inf)}, "
                f"duplicate_behavior_differences={len(dupes_diff)}"
            ),
            details="See parent_item_only_in_domo.csv, parent_item_only_in_informatica.csv, duplicate_behavior_differences.csv if generated.",
        )

    def _validate_parent_item_string_behavior(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_COLUMN not in domo_df.columns or KEY_COLUMN not in inf_df.columns:
            return

        issues: List[Dict[str, str]] = []
        common = sorted(set(domo_df[KEY_COLUMN].tolist()) & set(inf_df[KEY_COLUMN].tolist()))
        inf_set = set(inf_df[KEY_COLUMN].tolist())

        for value in common:
            if not isinstance(value, str):
                issues.append({KEY_COLUMN: str(value), "issue": "Non-string value detected"})
                continue

            if "e+" in value.lower() or "e-" in value.lower():
                issues.append({KEY_COLUMN: value, "issue": "Scientific notation detected"})

            if value != value.strip():
                issues.append({KEY_COLUMN: value, "issue": "Leading or trailing whitespace detected"})

            if value.isdigit() and len(value) > 1 and value.startswith("0") and value not in inf_set:
                issues.append({KEY_COLUMN: value, "issue": "Leading zero loss suspected"})

        status = "WARNING" if issues else "PASS"
        if issues:
            self.evidence["parent_item_string_issues"] = pd.DataFrame(issues)

        self.add_check(
            "PRICES-007",
            "Verify that Parent_Item_Code preserves string behavior, leading zeros, and original formatting in Informatica",
            status,
            "Datatype",
            field=KEY_COLUMN,
            expected="String comparison; no cast; no format loss",
            actual="No formatting issue detected" if status == "PASS" else f"{len(issues)} format observations",
            details="In compare mode, these observations do not fail equivalence. See parent_item_string_issues.csv if generated.",
            evidence_file="parent_item_string_issues.csv" if issues else "",
        )

    def _validate_blank_whitespace_behavior(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        common_cols = [c for c in domo_df.columns if c in inf_df.columns]
        count_results = []
        row_level_results = []

        # Paso 1: conteo rápido para todas las columnas
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

        # Paso 2: row-level solo para columnas que ya mostraron diferencia
        if STRICT_BLANK_ROW_LEVEL_MODE and columns_with_count_differences and KEY_COLUMN in domo_df.columns and KEY_COLUMN in inf_df.columns:
            domo_unique = domo_df.drop_duplicates(subset=[KEY_COLUMN], keep="first").copy()
            inf_unique = inf_df.drop_duplicates(subset=[KEY_COLUMN], keep="first").copy()

            domo_indexed = domo_unique.set_index(KEY_COLUMN, drop=False)
            inf_indexed = inf_unique.set_index(KEY_COLUMN, drop=False)

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
                                    KEY_COLUMN: key,
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
            "PRICES-008",
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

    def _parse_field_structure(self, field_name: str, value: str, country: str) -> List[str]:
        if field_name.startswith("Cost_Center_"):
            parsed = parse_cost_center(value)
            errors = []
            if not parsed["blank"] and any(token == "" for token in parsed["tokens"]):
                errors.append("Empty token in pipe list")
            return errors

        if field_name.startswith("POS_Sign_Price_"):
            parsed = parse_country_store_metric(value, expected_country=country, kind="price")
            return parsed["errors"]

        if field_name.startswith("Sell_Price_Effective_Date_") or field_name.startswith("Sell_Price_Expired_Date_"):
            parsed = parse_country_store_metric(value, expected_country=country, kind="date")
            return parsed["errors"]

        return []

    def _validate_country_fields(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_COLUMN not in domo_df.columns or KEY_COLUMN not in inf_df.columns:
            return

        print("    Preparing indexed data for country validation...")

        domo_unique = domo_df.drop_duplicates(subset=[KEY_COLUMN], keep="first").copy()
        inf_unique = inf_df.drop_duplicates(subset=[KEY_COLUMN], keep="first").copy()

        domo_indexed = domo_unique.set_index(KEY_COLUMN, drop=False)
        inf_indexed = inf_unique.set_index(KEY_COLUMN, drop=False)

        common_keys = sorted(set(domo_indexed.index) & set(inf_indexed.index))
        print(f"    Common unique keys for country checks: {len(common_keys):,}")

        domo_rows = domo_indexed.to_dict(orient="index")
        inf_rows = inf_indexed.to_dict(orient="index")

        field_diff_records: List[Dict[str, str]] = []
        token_structure_issues: List[Dict[str, str]] = []
        cross_field_failures: List[Dict[str, str]] = []

        for idx, country in enumerate(PRICE_COUNTRIES, start=1):
            print(f"    [{idx}/{len(PRICE_COUNTRIES)}] Country {country}...")

            fields = [template.format(country=country) for template in COUNTRY_COMPARE_FIELDS]
            missing_fields = [f for f in fields if f not in domo_df.columns or f not in inf_df.columns]

            if missing_fields:
                self.add_check(
                    f"PRICES-{country}-MISS",
                    f"Verify required country fields exist for {country}",
                    "FAIL",
                    "Country Content",
                    country=country,
                    field="; ".join(fields),
                    expected="All country-specific fields exist in both files",
                    actual=f"Missing fields: {len(missing_fields)}",
                    details=", ".join(missing_fields),
                )
                continue

            for field_name in fields:
                mismatch_count = 0
                structure_observations = 0

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
                                    KEY_COLUMN: key,
                                    "country": country,
                                    "field": field_name,
                                    "domo": domo_val,
                                    "informatica": inf_val,
                                }
                            )

                    # En modo compare, las observaciones estructurales no deben bloquear equivalencia
                    inf_errors = self._parse_field_structure(field_name, inf_val, country)
                    domo_errors = self._parse_field_structure(field_name, domo_val, country) if STRICT_STRUCTURE_BOTH_SIDES_MODE else []

                    if inf_errors or domo_errors:
                        structure_observations += 1
                        if len(token_structure_issues) < MAX_EVIDENCE_ROWS:
                            token_structure_issues.append(
                                {
                                    KEY_COLUMN: key,
                                    "country": country,
                                    "field": field_name,
                                    "domo_issue": " | ".join(domo_errors) if domo_errors else "",
                                    "informatica_issue": " | ".join(inf_errors) if inf_errors else "",
                                    "domo_value": domo_val,
                                    "informatica_value": inf_val,
                                }
                            )

                status = "PASS" if mismatch_count == 0 else "FAIL"
                category = "Country Content" if not any(field_name.startswith(prefix) for prefix in CODE_FIELD_PREFIXES) else "Country Codes"

                if field_name.startswith("POS_Sign_Price_"):
                    expected = "Exact raw value + COUNTRY~STORE:PRICE structure + decimals + token order/count + blank handling"
                elif field_name.startswith("Sell_Price_Effective_Date_") or field_name.startswith("Sell_Price_Expired_Date_"):
                    expected = "Exact raw value + COUNTRY~STORE:DATE structure + YYYY-MM-DD format + token order/count + blank handling"
                elif field_name.startswith("Cost_Center_"):
                    expected = "Exact raw value + delimiter '|' + token count + token order + blank handling"
                else:
                    expected = "Exact raw value match"

                details = ""
                if structure_observations > 0:
                    details = (
                        f"{structure_observations} structure observation(s) detected, but in compare mode "
                        f"only raw differences block equivalence. See token_structure_issues.csv"
                    )

                self.add_check(
                    f"PRICES-{country}-{field_name}",
                    f"Verify that {field_name} is preserved exactly in Informatica",
                    status,
                    category,
                    country=country,
                    field=field_name,
                    expected=expected,
                    actual="Matched" if status == "PASS" else f"record_differences={mismatch_count}",
                    details=details,
                    evidence_file="country_field_differences.csv" if mismatch_count > 0 else ("token_structure_issues.csv" if structure_observations > 0 else ""),
                )

            currency_field = COUNTRY_FIELD_TEMPLATES["currency_code"].format(country=country)
            country_code_field = COUNTRY_FIELD_TEMPLATES["country_code"].format(country=country)

            code_issues = []
            code_diffs = 0

            for key in common_keys:
                row_a = domo_rows[key]
                row_b = inf_rows[key]

                if row_a[currency_field] != row_b[currency_field] or row_a[country_code_field] != row_b[country_code_field]:
                    code_diffs += 1

                ok_currency, currency_msg = validate_code_value(row_b[currency_field])
                ok_country, country_msg = validate_code_value(row_b[country_code_field], expected_country=country)

                if not ok_currency or not ok_country:
                    if len(code_issues) < MAX_EVIDENCE_ROWS:
                        code_issues.append(
                            {
                                KEY_COLUMN: key,
                                "country": country,
                                "currency_value": row_b[currency_field],
                                "currency_issue": currency_msg,
                                "country_code_value": row_b[country_code_field],
                                "country_issue": country_msg,
                            }
                        )

            if code_issues:
                self.evidence[f"code_issues_{country}"] = pd.DataFrame(code_issues)

            self.add_check(
                f"PRICES-{country}-CODES",
                f"Verify that Currency_Code_{country} and Country_Code_{country} are preserved exactly in Informatica",
                "PASS" if code_diffs == 0 else "FAIL",
                "Country Codes",
                country=country,
                field=f"Currency_Code_{country}; Country_Code_{country}",
                expected="Exact value + expected code format + uppercase + blank handling",
                actual="Matched" if code_diffs == 0 else f"raw_differences={code_diffs}",
                details=(
                    f"{len(code_issues)} code format observation(s) detected, but in compare mode only raw differences block equivalence."
                    if code_issues
                    else "Validated against regex and expected country code."
                ),
                evidence_file=(f"code_issues_{country}.csv" if code_issues and code_diffs == 0 else (f"code_issues_{country}.csv" if code_diffs else "")),
            )

            cost_center_field = COUNTRY_FIELD_TEMPLATES["cost_center"].format(country=country)
            price_field = COUNTRY_FIELD_TEMPLATES["pos_sign_price"].format(country=country)
            eff_field = COUNTRY_FIELD_TEMPLATES["effective_date"].format(country=country)
            exp_field = COUNTRY_FIELD_TEMPLATES["expired_date"].format(country=country)

            cross_file_differences = 0
            internal_alignment_observations = 0

            for key in common_keys:
                row_a = domo_rows[key]
                row_b = inf_rows[key]

                domo_cost = extract_store_sequence_from_cost_center(row_a[cost_center_field])
                domo_price = extract_store_sequence_from_structured(row_a[price_field], expected_country=country, kind="price")
                domo_eff = extract_store_sequence_from_structured(row_a[eff_field], expected_country=country, kind="date")
                domo_exp = extract_store_sequence_from_structured(row_a[exp_field], expected_country=country, kind="date")

                inf_cost = extract_store_sequence_from_cost_center(row_b[cost_center_field])
                inf_price = extract_store_sequence_from_structured(row_b[price_field], expected_country=country, kind="price")
                inf_eff = extract_store_sequence_from_structured(row_b[eff_field], expected_country=country, kind="date")
                inf_exp = extract_store_sequence_from_structured(row_b[exp_field], expected_country=country, kind="date")

                domo_alignment = [domo_cost, domo_price, domo_eff, domo_exp]
                inf_alignment = [inf_cost, inf_price, inf_eff, inf_exp]

                domo_internally_aligned = all(seq == domo_cost for seq in domo_alignment)
                inf_internally_aligned = all(seq == inf_cost for seq in inf_alignment)
                between_files_aligned = domo_alignment == inf_alignment

                if not between_files_aligned:
                    cross_file_differences += 1
                    if len(cross_field_failures) < MAX_EVIDENCE_ROWS:
                        cross_field_failures.append(
                            {
                                KEY_COLUMN: key,
                                "country": country,
                                "domo_cost_center_stores": "|".join(domo_cost),
                                "domo_price_stores": "|".join(domo_price),
                                "domo_effective_stores": "|".join(domo_eff),
                                "domo_expired_stores": "|".join(domo_exp),
                                "informatica_cost_center_stores": "|".join(inf_cost),
                                "informatica_price_stores": "|".join(inf_price),
                                "informatica_effective_stores": "|".join(inf_eff),
                                "informatica_expired_stores": "|".join(inf_exp),
                            }
                        )
                elif not domo_internally_aligned or not inf_internally_aligned:
                    internal_alignment_observations += 1
                    if len(cross_field_failures) < MAX_EVIDENCE_ROWS:
                        cross_field_failures.append(
                            {
                                KEY_COLUMN: key,
                                "country": country,
                                "domo_cost_center_stores": "|".join(domo_cost),
                                "domo_price_stores": "|".join(domo_price),
                                "domo_effective_stores": "|".join(domo_eff),
                                "domo_expired_stores": "|".join(domo_exp),
                                "informatica_cost_center_stores": "|".join(inf_cost),
                                "informatica_price_stores": "|".join(inf_price),
                                "informatica_effective_stores": "|".join(inf_eff),
                                "informatica_expired_stores": "|".join(inf_exp),
                            }
                        )

            self.add_check(
                f"PRICES-{country}-XFIELD",
                f"Verify that cross-field store alignment is preserved for country {country} in Informatica",
                "PASS" if cross_file_differences == 0 else "FAIL",
                "Cross-Field",
                country=country,
                field=f"Cost_Center_{country}; POS_Sign_Price_{country}; Sell_Price_Effective_Date_{country}; Sell_Price_Expired_Date_{country}; Currency_Code_{country}; Country_Code_{country}",
                expected="Store count and store-reference consistency across cost center, price, effective date, and expiration date fields",
                actual="Matched" if cross_file_differences == 0 else f"{cross_file_differences} keys with between-file alignment differences",
                details=(
                    f"{internal_alignment_observations} internal alignment observation(s) detected, but in compare mode these do not fail equivalence."
                    if internal_alignment_observations > 0
                    else ""
                ),
                evidence_file="cross_field_alignment_failures.csv" if (cross_file_differences > 0 or internal_alignment_observations > 0) else "",
            )

        if field_diff_records:
            self.evidence["country_field_differences"] = pd.DataFrame(field_diff_records)
        if token_structure_issues:
            self.evidence["token_structure_issues"] = pd.DataFrame(token_structure_issues)
        if cross_field_failures:
            self.evidence["cross_field_alignment_failures"] = pd.DataFrame(cross_field_failures)

    def _validate_overall_decision(self) -> None:
        blocking_statuses = [c.status for c in self.checks if c.status == "FAIL"]
        self.add_check(
            "PRICES-999",
            "Verify that the full Informatica prices output is functionally equivalent to Domo across all countries",
            "PASS" if not blocking_statuses else "FAIL",
            "Overall Reconciliation",
            field="All countries",
            expected="No unresolved variance exists across the country-level validation scope; Informatica is functionally equivalent to Domo for the prices dataset.",
            actual="Equivalent" if not blocking_statuses else f"{len(blocking_statuses)} blocking failures detected",
        )

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

    @staticmethod
    def _duplicate_counts(values: List[str]) -> Dict[str, int]:
        counter = Counter(values)
        return {key: count for key, count in counter.items() if count > 1}

    @staticmethod
    def _diff_duplicate_behavior(domo_dupes: Dict[str, int], inf_dupes: Dict[str, int]) -> List[Dict[str, object]]:
        rows = []
        for key in sorted(set(domo_dupes.keys()) | set(inf_dupes.keys())):
            domo_count = domo_dupes.get(key, 0)
            inf_count = inf_dupes.get(key, 0)
            if domo_count != inf_count:
                rows.append(
                    {
                        KEY_COLUMN: key,
                        "domo_duplicate_count": domo_count,
                        "informatica_duplicate_count": inf_count,
                    }
                )
        return rows