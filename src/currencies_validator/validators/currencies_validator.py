from __future__ import annotations

from collections import Counter
from typing import Dict, List

import pandas as pd

from currencies_validator.config.currencies_config import (
    DATASET_NAME,
    EXPECTED_BASELINE_COLUMN_COUNT,
    EXPECTED_COLUMNS_BASELINE,
    KEY_COUNTRY_COLUMN,
    KEY_CURRENCY_COLUMN,
    KEY_DATE_COLUMN,
    KEY_WAREHOUSE_COLUMN,
    MAX_EVIDENCE_ROWS,
    SNAPSHOT_CANDIDATE_COLUMNS,
    STRICT_SCHEMA_MODE,
    STRICT_SNAPSHOT_MODE,
    STRICT_UNIVERSE_MODE,
)
from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun


class CurrenciesValidator:
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

        print("  [3/8] Approved comparison universe...")
        self._validate_approved_comparison_universe(domo_df, inf_df)

        print("  [4/8] Record count validation...")
        self._validate_record_count_current_last_year(domo_df, inf_df)

        print("  [5/8] Country-level validation...")
        self._validate_country_level(domo_df, inf_df)

        print("  [6/8] Key population validation...")
        self._validate_key_population(domo_df, inf_df)

        print("  [7/8] Null / parse observations...")
        self._validate_null_and_parse_observations(domo_df, inf_df)

        print("  [8/8] Overall decision...")
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
                "CURRENCIES-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
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
                "CURRENCIES-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
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
            domo_vals = sorted(set(domo_df[col].dropna().tolist()))
            inf_vals = sorted(set(inf_df[col].dropna().tolist()))

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
                        "domo": " | ".join(map(str, domo_vals[:10])),
                        "informatica": " | ".join(map(str, inf_vals[:10])),
                    }
                )

        if multiplicity_issues:
            self.evidence["currencies_snapshot_multiplicity_issues"] = pd.DataFrame(multiplicity_issues)

        if mismatches:
            self.evidence["currencies_snapshot_mismatches"] = pd.DataFrame(mismatches)
            self.add_check(
                "CURRENCIES-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "FAIL",
                "File Control",
                field="Business date or approved comparison window",
                expected="Same metadata value set",
                actual=f"{len(mismatches)} mismatching metadata columns",
                details="See currencies_snapshot_mismatches.csv",
                evidence_file="currencies_snapshot_mismatches.csv",
            )
            return

        if multiplicity_issues:
            self.add_check(
                "CURRENCIES-001",
                "Verify that Domo and Informatica files belong to the same business snapshot",
                "WARNING",
                "File Control",
                field="Business date or approved comparison window",
                expected="Exactly one distinct snapshot value per comparable metadata column",
                actual=f"{len(multiplicity_issues)} columns contain multiple values",
                details="Values match between files, but multiplicity was detected.",
                evidence_file="currencies_snapshot_multiplicity_issues.csv",
            )
            return

        self.add_check(
            "CURRENCIES-001",
            "Verify that Domo and Informatica files belong to the same business snapshot",
            "PASS",
            "File Control",
            field="Business date or approved comparison window",
            expected="Same metadata value set",
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
                "CURRENCIES-002A",
                "Verify baseline definition contains the complete expected Currencies schema",
                "FAIL",
                "Schema",
                field="Baseline configuration",
                expected=f"{EXPECTED_BASELINE_COLUMN_COUNT} columns",
                actual=f"{len(baseline)} columns",
                details="currencies_config.py EXPECTED_COLUMNS_BASELINE must be corrected.",
            )
        else:
            self.add_check(
                "CURRENCIES-002A",
                "Verify baseline definition contains the complete expected Currencies schema",
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
            self.evidence["currencies_missing_columns_in_informatica"] = pd.DataFrame({"column": missing_in_inf})
        if extra_in_inf:
            self.evidence["currencies_extra_columns_in_informatica"] = pd.DataFrame({"column": extra_in_inf})
        if missing_in_domo:
            self.evidence["currencies_missing_columns_in_domo"] = pd.DataFrame({"column": missing_in_domo})
        if extra_in_domo:
            self.evidence["currencies_extra_columns_in_domo"] = pd.DataFrame({"column": extra_in_domo})

        if not exact_order_inf or not exact_order_domo:
            order_rows = []
            max_len = max(len(baseline), len(inf_cols), len(domo_cols))
            for idx in range(max_len):
                exp = baseline[idx] if idx < len(baseline) else ""
                act_inf = inf_cols[idx] if idx < len(inf_cols) else ""
                act_domo = domo_cols[idx] if idx < len(domo_cols) else ""
                if exp != act_inf or exp != act_domo:
                    order_rows.append(
                        {
                            "position": idx + 1,
                            "expected": exp,
                            "domo_actual": act_domo,
                            "informatica_actual": act_inf,
                        }
                    )
            self.evidence["currencies_column_order_differences"] = pd.DataFrame(order_rows)

        self.add_check(
            "CURRENCIES-002",
            "Verify that Informatica preserves the complete Domo schema for the currencies file",
            "PASS" if not missing_in_inf and not missing_in_domo else "FAIL",
            "Schema",
            field="All columns",
            expected=f"All baseline columns present ({len(baseline)})",
            actual=f"Missing in Domo: {len(missing_in_domo)}, Missing in Informatica: {len(missing_in_inf)}",
            details=(
                f"Domo missing: {', '.join(missing_in_domo[:10])} | "
                f"Informatica missing: {', '.join(missing_in_inf[:10])}"
            ).strip(" |") if (missing_in_domo or missing_in_inf) else "No missing columns",
            evidence_file="currencies_missing_columns_in_informatica.csv" if missing_in_inf else ("currencies_missing_columns_in_domo.csv" if missing_in_domo else ""),
        )

        self.add_check(
            "CURRENCIES-003",
            "Verify that Informatica does not introduce unexpected columns or alter header naming",
            "PASS" if not extra_in_inf and not extra_in_domo and set(inf_cols) == set(baseline) and set(domo_cols) == set(baseline) else "FAIL",
            "Schema",
            field="All columns",
            expected="No extras + exact header text",
            actual=f"Extra in Domo: {len(extra_in_domo)}, Extra in Informatica: {len(extra_in_inf)}",
            details=(
                f"Domo extras: {', '.join(extra_in_domo[:10])} | "
                f"Informatica extras: {', '.join(extra_in_inf[:10])}"
            ).strip(" |") if (extra_in_domo or extra_in_inf) else "No unexpected columns",
            evidence_file="currencies_extra_columns_in_informatica.csv" if extra_in_inf else ("currencies_extra_columns_in_domo.csv" if extra_in_domo else ""),
        )

        self.add_check(
            "CURRENCIES-004",
            "Verify that column order in Informatica matches the Domo baseline",
            "PASS" if exact_order_inf and exact_order_domo else "FAIL",
            "Schema",
            field="All columns",
            expected="Exact header sequence",
            actual="Matched" if exact_order_inf and exact_order_domo else f"Domo order ok={exact_order_domo}, Informatica order ok={exact_order_inf}",
            details="See currencies_column_order_differences.csv" if not exact_order_inf or not exact_order_domo else "",
            evidence_file="currencies_column_order_differences.csv" if not exact_order_inf or not exact_order_domo else "",
        )

    # -------------------------------------------------------------------------
    # Approved universe
    # -------------------------------------------------------------------------
    def _validate_approved_comparison_universe(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_DATE_COLUMN not in domo_df.columns or KEY_DATE_COLUMN not in inf_df.columns:
            self.add_check(
                "CURRENCIES-005",
                "Verify that Domo and Informatica currency files belong to the same approved comparison universe",
                "FAIL",
                "File Control",
                field=KEY_DATE_COLUMN,
                expected="Column exists in both files",
                actual="Missing in one or both files",
            )
            return

        domo_dates = pd.to_datetime(domo_df[KEY_DATE_COLUMN], errors="coerce")
        inf_dates = pd.to_datetime(inf_df[KEY_DATE_COLUMN], errors="coerce")

        domo_valid = domo_dates.dropna()
        inf_valid = inf_dates.dropna()

        if domo_valid.empty or inf_valid.empty:
            self.add_check(
                "CURRENCIES-005",
                "Verify that Domo and Informatica currency files belong to the same approved comparison universe",
                "FAIL",
                "File Control",
                field=KEY_DATE_COLUMN,
                expected="Valid dates in both files",
                actual="No valid dates found in one or both files",
            )
            return

        today = pd.Timestamp.today()
        allowed_years = {today.year, today.year - 1}

        domo_all_years = sorted(domo_valid.dt.year.unique().tolist())
        inf_all_years = sorted(inf_valid.dt.year.unique().tolist())

        domo_approved_years = sorted([y for y in domo_all_years if y in allowed_years])
        inf_approved_years = sorted([y for y in inf_all_years if y in allowed_years])

        domo_filtered = domo_valid[domo_valid.dt.year.isin(allowed_years)]
        inf_filtered = inf_valid[inf_valid.dt.year.isin(allowed_years)]

        missing_approved_in_domo = sorted(list(allowed_years - set(domo_approved_years)))
        missing_approved_in_inf = sorted(list(allowed_years - set(inf_approved_years)))

        same_approved_years = domo_approved_years == inf_approved_years
        has_approved_data = not domo_filtered.empty and not inf_filtered.empty

        status = "PASS" if same_approved_years and has_approved_data else "FAIL"

        details = (
            f"Approved years evaluated: {sorted(list(allowed_years))}. "
            f"DOMO approved years present={domo_approved_years}; "
            f"Informatica approved years present={inf_approved_years}. "
            f"DOMO approved rows={len(domo_filtered)}; "
            f"Informatica approved rows={len(inf_filtered)}."
        )

        if missing_approved_in_domo or missing_approved_in_inf:
            details += (
                f" Missing approved years -> "
                f"DOMO: {missing_approved_in_domo or 'none'}, "
                f"Informatica: {missing_approved_in_inf or 'none'}."
            )

        if domo_all_years != inf_all_years:
            extra_year_rows = []
            only_in_domo = sorted(list(set(domo_all_years) - set(inf_all_years)))
            only_in_inf = sorted(list(set(inf_all_years) - set(domo_all_years)))

            for year in only_in_domo:
                extra_year_rows.append({"source": "DOMO", "year": year, "issue": "Year not present in Informatica"})
            for year in only_in_inf:
                extra_year_rows.append({"source": "INFORMATICA", "year": year, "issue": "Year not present in DOMO"})

            if extra_year_rows:
                self.evidence["currencies_outside_universe_year_differences"] = pd.DataFrame(extra_year_rows)

        self.add_check(
            "CURRENCIES-005",
            "Verify that Domo and Informatica currency files belong to the same approved comparison universe",
            status,
            "File Control",
            field=KEY_DATE_COLUMN,
            expected=f"Both files contain the same approved comparison universe for years {today.year - 1} and {today.year}",
            actual=f"DOMO approved years={domo_approved_years}; Informatica approved years={inf_approved_years}",
            details=details,
            evidence_file="currencies_outside_universe_year_differences.csv"
            if "currencies_outside_universe_year_differences" in self.evidence
            else "",
        )

    # -------------------------------------------------------------------------
    # Record count within approved universe
    # -------------------------------------------------------------------------
    def _validate_record_count_current_last_year(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_DATE_COLUMN not in domo_df.columns or KEY_DATE_COLUMN not in inf_df.columns:
            self.add_check(
                "CURRENCIES-006",
                "Verify that the total record count in Informatica matches Domo for Current + Last Year",
                "FAIL",
                "Volume",
                field=KEY_DATE_COLUMN,
                expected="Date column available in both files",
                actual="Missing in one or both files",
            )
            return

        today = pd.Timestamp.today()
        allowed_years = {today.year, today.year - 1}

        domo_dates = pd.to_datetime(domo_df[KEY_DATE_COLUMN], errors="coerce")
        inf_dates = pd.to_datetime(inf_df[KEY_DATE_COLUMN], errors="coerce")

        domo_filtered = domo_df.loc[domo_dates.dt.year.isin(allowed_years).fillna(False)].copy()
        inf_filtered = inf_df.loc[inf_dates.dt.year.isin(allowed_years).fillna(False)].copy()

        status = "PASS" if len(domo_filtered) == len(inf_filtered) else "FAIL"

        self.add_check(
            "CURRENCIES-006",
            "Verify that the total record count in Informatica matches Domo for Current + Last Year",
            status,
            "Volume",
            field="All records in approved universe",
            expected=str(len(domo_filtered)),
            actual=str(len(inf_filtered)),
            details=f"Compared only rows where {KEY_DATE_COLUMN} belongs to years {today.year - 1} and {today.year}.",
        )

    # -------------------------------------------------------------------------
    # Country-level validation
    # -------------------------------------------------------------------------
    def _validate_country_level(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if KEY_COUNTRY_COLUMN not in domo_df.columns or KEY_COUNTRY_COLUMN not in inf_df.columns:
            self.add_check(
                "CURRENCIES-006C",
                "Verify that currencies validation covers all countries present in Domo and Informatica",
                "FAIL",
                "Country Content",
                field=KEY_COUNTRY_COLUMN,
                expected="Country column exists in both files",
                actual="Missing in one or both files",
            )
            return

        domo_countries = sorted(set(domo_df[KEY_COUNTRY_COLUMN].dropna().astype(str).tolist()))
        inf_countries = sorted(set(inf_df[KEY_COUNTRY_COLUMN].dropna().astype(str).tolist()))

        only_domo = sorted(set(domo_countries) - set(inf_countries))
        only_inf = sorted(set(inf_countries) - set(domo_countries))

        if only_domo or only_inf:
            rows = []
            for c in only_domo:
                rows.append({"source": "DOMO_ONLY", "country": c})
            for c in only_inf:
                rows.append({"source": "INFORMATICA_ONLY", "country": c})
            self.evidence["currencies_country_presence_differences"] = pd.DataFrame(rows)

        self.add_check(
            "CURRENCIES-006C",
            "Verify that currencies validation covers all countries present in Domo and Informatica",
            "PASS" if not only_domo and not only_inf else "FAIL",
            "Country Content",
            field=KEY_COUNTRY_COLUMN,
            expected="Same country population in both files",
            actual=f"only_in_domo={len(only_domo)}, only_in_informatica={len(only_inf)}",
            details=f"DOMO countries={len(domo_countries)}, Informatica countries={len(inf_countries)}",
            evidence_file="currencies_country_presence_differences.csv" if (only_domo or only_inf) else "",
        )

        common_countries = sorted(set(domo_countries) & set(inf_countries))

        print("    Preparing country-level validation...")
        print(f"    Common countries for checks: {len(common_countries):,}")

        today = pd.Timestamp.today()
        allowed_years = {today.year, today.year - 1}

        country_row_diff_rows = []
        country_key_diff_rows = []

        for idx, country in enumerate(common_countries, start=1):
            print(f"    [{idx}/{len(common_countries)}] Country {country}...")

            domo_country = domo_df[domo_df[KEY_COUNTRY_COLUMN].astype(str) == country].copy()
            inf_country = inf_df[inf_df[KEY_COUNTRY_COLUMN].astype(str) == country].copy()

            domo_dates = pd.to_datetime(domo_country[KEY_DATE_COLUMN], errors="coerce")
            inf_dates = pd.to_datetime(inf_country[KEY_DATE_COLUMN], errors="coerce")

            domo_filtered = domo_country.loc[domo_dates.dt.year.isin(allowed_years).fillna(False)].copy()
            inf_filtered = inf_country.loc[inf_dates.dt.year.isin(allowed_years).fillna(False)].copy()

            print(
                f"      Current+LastYear rows -> DOMO: {len(domo_filtered):,}, "
                f"INFORMATICA: {len(inf_filtered):,}"
            )

            row_status = "PASS" if len(domo_filtered) == len(inf_filtered) else "FAIL"
            self.add_check(
                f"CURRENCIES-{country}-ROW",
                f"Verify Current + Last Year record count matches for country {country}",
                row_status,
                "Country Content",
                country=country,
                field="All records in approved universe",
                expected=str(len(domo_filtered)),
                actual=str(len(inf_filtered)),
                details=f"Compared only rows for country {country} in years {today.year - 1} and {today.year}.",
            )

            if row_status == "FAIL":
                country_row_diff_rows.append(
                    {
                        "country": country,
                        "domo_rows_current_last_year": len(domo_filtered),
                        "informatica_rows_current_last_year": len(inf_filtered),
                    }
                )

            domo_keys = set(
                zip(
                    domo_country[KEY_DATE_COLUMN].astype(str),
                    domo_country[KEY_WAREHOUSE_COLUMN].astype(str),
                    domo_country[KEY_CURRENCY_COLUMN].astype(str),
                )
            )
            inf_keys = set(
                zip(
                    inf_country[KEY_DATE_COLUMN].astype(str),
                    inf_country[KEY_WAREHOUSE_COLUMN].astype(str),
                    inf_country[KEY_CURRENCY_COLUMN].astype(str),
                )
            )

            only_country_domo = sorted(domo_keys - inf_keys)
            only_country_inf = sorted(inf_keys - domo_keys)

            print(
                f"      Key population diffs -> "
                f"only_in_domo={len(only_country_domo)}, "
                f"only_in_informatica={len(only_country_inf)}"
            )

            key_status = "PASS" if not only_country_domo and not only_country_inf else "FAIL"
            self.add_check(
                f"CURRENCIES-{country}-KEY",
                f"Verify date, warehouse, and currency population matches for country {country}",
                key_status,
                "Country Content",
                country=country,
                field=f"{KEY_DATE_COLUMN}; {KEY_WAREHOUSE_COLUMN}; {KEY_CURRENCY_COLUMN}",
                expected="Set equality within country",
                actual=f"only_in_domo={len(only_country_domo)}, only_in_informatica={len(only_country_inf)}",
                details="Country-scoped key population comparison.",
            )

            for item in only_country_domo[:MAX_EVIDENCE_ROWS]:
                country_key_diff_rows.append(
                    {
                        "country": country,
                        "source": "DOMO_ONLY",
                        KEY_DATE_COLUMN: item[0],
                        KEY_WAREHOUSE_COLUMN: item[1],
                        KEY_CURRENCY_COLUMN: item[2],
                    }
                )
            for item in only_country_inf[:MAX_EVIDENCE_ROWS]:
                country_key_diff_rows.append(
                    {
                        "country": country,
                        "source": "INFORMATICA_ONLY",
                        KEY_DATE_COLUMN: item[0],
                        KEY_WAREHOUSE_COLUMN: item[1],
                        KEY_CURRENCY_COLUMN: item[2],
                    }
                )

        if country_row_diff_rows:
            self.evidence["currencies_country_row_count_differences"] = pd.DataFrame(country_row_diff_rows)

        if country_key_diff_rows:
            self.evidence["currencies_country_key_differences"] = pd.DataFrame(country_key_diff_rows)

    # -------------------------------------------------------------------------
    # Key population
    # -------------------------------------------------------------------------
    def _validate_key_population(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        key_cols = [
            KEY_COUNTRY_COLUMN,
            KEY_DATE_COLUMN,
            KEY_WAREHOUSE_COLUMN,
            KEY_CURRENCY_COLUMN,
        ]

        for idx, key_col in enumerate(key_cols, start=1):
            check_id = f"CURRENCIES-007{chr(96 + idx)}"

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
                self.evidence[f"currencies_{key_col}_only_in_domo"] = pd.DataFrame({key_col: only_domo[:MAX_EVIDENCE_ROWS]})
            if only_inf:
                self.evidence[f"currencies_{key_col}_only_in_informatica"] = pd.DataFrame({key_col: only_inf[:MAX_EVIDENCE_ROWS]})

            status = "PASS" if not only_domo and not only_inf else "FAIL"

            self.add_check(
                check_id,
                f"Verify that the {key_col} population matches exactly between Domo and Informatica",
                status,
                "Population",
                field=key_col,
                expected="Set equality",
                actual=f"only_in_domo={len(only_domo)}, only_in_informatica={len(only_inf)}",
                details="See evidence files if generated.",
            )

    # -------------------------------------------------------------------------
    # Null / parse observations
    # -------------------------------------------------------------------------
    def _validate_null_and_parse_observations(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        observations = []

        for source_name, df in [("DOMO", domo_df), ("INFORMATICA", inf_df)]:
            null_counts = {col: int(cnt) for col, cnt in df.isna().sum().to_dict().items() if int(cnt) > 0}
            for col, cnt in null_counts.items():
                observations.append(
                    {
                        "source": source_name,
                        "column": col,
                        "issue": "Null values detected",
                        "count": cnt,
                    }
                )

            if KEY_DATE_COLUMN in df.columns:
                invalid_dates = int(pd.to_datetime(df[KEY_DATE_COLUMN], errors="coerce").isna().sum())
                if invalid_dates > 0:
                    observations.append(
                        {
                            "source": source_name,
                            "column": KEY_DATE_COLUMN,
                            "issue": "Invalid date values detected",
                            "count": invalid_dates,
                        }
                    )

        if observations:
            self.evidence["currencies_null_parse_observations"] = pd.DataFrame(observations)

        self.add_check(
            "CURRENCIES-008",
            "Verify currencies files do not contain unexpected null or date parsing issues",
            "WARNING" if observations else "PASS",
            "Quality Observations",
            field="All columns",
            expected="No unexpected null or parsing issues",
            actual="No observations" if not observations else f"{len(observations)} observation rows",
            details="Observations do not block equivalence unless tied to a failing scoped check.",
            evidence_file="currencies_null_parse_observations.csv" if observations else "",
        )

    # -------------------------------------------------------------------------
    # Overall
    # -------------------------------------------------------------------------
    def _validate_overall_decision(self) -> None:
        blocking_statuses = [c.status for c in self.checks if c.status == "FAIL"]
        self.add_check(
            "CURRENCIES-999",
            "Verify that the full Informatica currencies output is functionally equivalent to Domo for the approved comparison universe",
            "PASS" if not blocking_statuses else "FAIL",
            "Overall Reconciliation",
            field="All records",
            expected="No unresolved variance exists across schema, approved universe, volume, country-level content, and key population scope.",
            actual="Equivalent" if not blocking_statuses else f"{len(blocking_statuses)} blocking failures detected",
        )

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    def _build_summary(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> Dict[str, object]:
        status_counts = Counter(c.status for c in self.checks)

        domo_valid_dates = pd.to_datetime(domo_df[KEY_DATE_COLUMN], errors="coerce").dropna() if KEY_DATE_COLUMN in domo_df.columns else pd.Series(dtype="datetime64[ns]")
        inf_valid_dates = pd.to_datetime(inf_df[KEY_DATE_COLUMN], errors="coerce").dropna() if KEY_DATE_COLUMN in inf_df.columns else pd.Series(dtype="datetime64[ns]")

        today = pd.Timestamp.today()
        allowed_years = {today.year, today.year - 1}

        domo_current_last_year = (
            int(domo_valid_dates.dt.year.isin(allowed_years).sum()) if not domo_valid_dates.empty else 0
        )
        inf_current_last_year = (
            int(inf_valid_dates.dt.year.isin(allowed_years).sum()) if not inf_valid_dates.empty else 0
        )

        domo_country_count = int(domo_df[KEY_COUNTRY_COLUMN].dropna().astype(str).nunique()) if KEY_COUNTRY_COLUMN in domo_df.columns else 0
        inf_country_count = int(inf_df[KEY_COUNTRY_COLUMN].dropna().astype(str).nunique()) if KEY_COUNTRY_COLUMN in inf_df.columns else 0

        return {
            "dataset": DATASET_NAME,
            "domo_rows": len(domo_df),
            "informatica_rows": len(inf_df),
            "expected_columns": len(EXPECTED_COLUMNS_BASELINE),
            "actual_domo_columns": len(domo_df.columns),
            "actual_informatica_columns": len(inf_df.columns),
            "domo_date_min": str(domo_valid_dates.min().date()) if not domo_valid_dates.empty else "",
            "domo_date_max": str(domo_valid_dates.max().date()) if not domo_valid_dates.empty else "",
            "informatica_date_min": str(inf_valid_dates.min().date()) if not inf_valid_dates.empty else "",
            "informatica_date_max": str(inf_valid_dates.max().date()) if not inf_valid_dates.empty else "",
            "domo_rows_current_last_year": domo_current_last_year,
            "informatica_rows_current_last_year": inf_current_last_year,
            "domo_country_count": domo_country_count,
            "informatica_country_count": inf_country_count,
            "total_checks": len(self.checks),
            "passed": status_counts.get("PASS", 0),
            "failed": status_counts.get("FAIL", 0),
            "warnings": status_counts.get("WARNING", 0),
            "overall_status": "FAIL" if status_counts.get("FAIL", 0) else "PASS",
        }