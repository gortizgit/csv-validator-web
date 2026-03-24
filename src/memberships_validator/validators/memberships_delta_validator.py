from __future__ import annotations

from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple

import pandas as pd

from prices_validator.core.types import CheckResult, SnapshotContext, ValidationRun
from memberships_validator.config.memberships_delta_config import (
    ACCOUNT_STATUS_ALLOWED,
    ACTIVE_ACCOUNT_STATUS_CODES,
    ACTIVE_CARD_STATUS_CODES,
    ALLOWED_DOMO_EXTRA_COLUMNS,
    ALLOW_WARNING_WHEN_NOT_COMPARABLE,
    BOOLEAN_LIKE_COLUMNS,
    BOTH_FILES_ARE_CONSOLIDATED,
    CARD_KEY_COLUMNS,
    CARD_STATUS_ALLOWED,
    CLUB_TO_COUNTRY_ALPHA2,
    CONSOLIDATED_COMPARISON_WINDOW_MINUTES,
    CONSOLIDATED_SNAPSHOT_TOLERANCE_MINUTES,
    COUNTRY_ALIASES,
    COUNTRY_ALPHA2_CODES,
    COUNTRY_ALPHA2_TO_ISO3,
    COUNTRY_COLUMNS,
    COUNTRY_ISO3_CODES,
    DATASET_NAME,
    DATE_COLUMNS,
    DELTA_ALLOW_COUNTRY_COUNT_DIFFERENCE,
    DELTA_ALLOW_KEY_SET_DIFFERENCE,
    DELTA_ALLOW_ROW_COUNT_DIFFERENCE,
    DELTA_ALLOW_TIMESTAMP_RANGE_DIFFERENCE,
    DELTA_SKIP_FULL_MULTISET_RECONCILIATION,
    DELTA_WINDOW_MINUTES,
    DISALLOWED_SPECIAL_CHARACTERS,
    DOMO_CONSOLIDATED_WINDOW_MINUTES,
    DOMO_IS_CONSOLIDATED,
    DOWNGRADE_MEMBERSHIP_TYPE_CODES,
    EXPECTED_BASELINE_COLUMN_COUNT,
    EXPECTED_COLUMNS_BASELINE,
    FAIL_ON_EMPTY_FILES,
    INTEGER_COLUMNS,
    INVALID_TEXT_VALUES,
    KEY_COLUMNS_PRIMARY,
    KEY_COLUMNS_SECONDARY,
    MAX_EVIDENCE_ROWS,
    MIN_COMMON_PRIMARY_KEYS_FOR_STRICT_COMPARISON,
    MODE_NAME,
    NUMERIC_COLUMNS,
    PLATINUM_ALLOWED_VALUES,
    PRIMARY_ALLOWED_VALUES,
    REQUIRED_NOT_BLANK_FIELDS,
    TIMESTAMP_COLUMN,
    TIMESTAMP_COMPARE_DATE_ONLY,
    TRANSITION_VALUE_COLUMNS,
    UPGRADE_MEMBERSHIP_TYPE_CODES,
    UPGRADE_PRICE_COLUMNS,
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

    def _trim_value(self, value: object) -> str:
        return self._trim_series(pd.Series([value])).iloc[0]

    def _parse_snapshot_datetime(self, value: str) -> pd.Timestamp | None:
        raw = (value or "").strip()
        if not raw:
            return None
        parsed = pd.to_datetime(raw, errors="coerce")
        return None if pd.isna(parsed) else parsed

    def _same_snapshot(self) -> bool:
        domo_snapshot = self._parse_snapshot_datetime(self.snapshot_context.domo_snapshot.strip())
        inf_snapshot = self._parse_snapshot_datetime(self.snapshot_context.informatica_snapshot.strip())
        return (
            domo_snapshot is not None
            and inf_snapshot is not None
            and domo_snapshot == inf_snapshot
        )

    def _snapshots_within_tolerance(self) -> bool:
        domo_snapshot = self._parse_snapshot_datetime(self.snapshot_context.domo_snapshot.strip())
        inf_snapshot = self._parse_snapshot_datetime(self.snapshot_context.informatica_snapshot.strip())

        if domo_snapshot is None or inf_snapshot is None:
            return False

        tolerance_minutes = (
            CONSOLIDATED_SNAPSHOT_TOLERANCE_MINUTES
            if BOTH_FILES_ARE_CONSOLIDATED
            else DELTA_WINDOW_MINUTES
        )
        diff_minutes = abs((domo_snapshot - inf_snapshot).total_seconds()) / 60.0
        return diff_minutes <= tolerance_minutes

    def _effective_window_minutes(self) -> int:
        return (
            CONSOLIDATED_COMPARISON_WINDOW_MINUTES
            if BOTH_FILES_ARE_CONSOLIDATED
            else DELTA_WINDOW_MINUTES
        )

    def _normalize_country_value(self, value: object) -> str:
        if value is None:
            return ""
        raw = str(value).strip()
        if not raw:
            return ""

        normalized_key = raw.upper().replace("_", " ").replace("-", " ")
        normalized_key = " ".join(normalized_key.split())
        normalized_key_compact = normalized_key.replace(" ", "")

        return COUNTRY_ALIASES.get(
            normalized_key,
            COUNTRY_ALIASES.get(normalized_key_compact, raw.upper()),
        )

    def _normalize_country_alpha2(self, value: object) -> str:
        normalized = self._normalize_country_value(value)
        return normalized if normalized in COUNTRY_ALPHA2_CODES else ""

    def _normalize_country_iso3(self, value: object) -> str:
        raw = self._trim_value(value).upper()
        if not raw:
            return ""

        if raw in COUNTRY_ISO3_CODES:
            return raw

        alpha2 = self._normalize_country_alpha2(raw)
        if not alpha2:
            return ""
        return COUNTRY_ALPHA2_TO_ISO3.get(alpha2, "")

    def _resolve_row_country(self, row: Dict[str, object]) -> str:
        normalized_values: List[str] = []

        for col in COUNTRY_COLUMNS:
            if col not in row:
                continue

            if col == "Country_Code_Full":
                iso3 = self._normalize_country_iso3(row.get(col, ""))
                if iso3:
                    alpha2 = COUNTRY_ISO3_CODES and next(
                        (k for k, v in COUNTRY_ALPHA2_TO_ISO3.items() if v == iso3),
                        "",
                    )
                    if alpha2:
                        normalized_values.append(alpha2)
            else:
                alpha2 = self._normalize_country_alpha2(row.get(col, ""))
                if alpha2:
                    normalized_values.append(alpha2)

        if not normalized_values:
            return ""

        counts = Counter(normalized_values)
        return counts.most_common(1)[0][0]

    def _country_alignment_issues(self, row: Dict[str, object], expected_country: str) -> List[str]:
        issues: List[str] = []

        country_code_raw = row.get("country_code", "")
        country_full_raw = row.get("Country_Code_Full", "")

        country_code_alpha2 = self._normalize_country_alpha2(country_code_raw)
        country_full_iso3 = self._normalize_country_iso3(country_full_raw)
        expected_iso3 = COUNTRY_ALPHA2_TO_ISO3.get(expected_country, "")

        if not country_code_alpha2:
            issues.append("country_code blank/invalid")
        elif country_code_alpha2 != expected_country:
            issues.append(f"country_code resolves to {country_code_alpha2}")

        if not country_full_iso3:
            issues.append("Country_Code_Full blank/invalid")
        elif country_full_iso3 != expected_iso3:
            issues.append(f"Country_Code_Full ISO3 expected {expected_iso3}, got {country_full_iso3}")

        club_code = self._trim_value(row.get("club_code", ""))
        expected_club_country = CLUB_TO_COUNTRY_ALPHA2.get(club_code)
        if expected_club_country and expected_club_country != expected_country:
            issues.append(f"club_code {club_code} maps to {expected_club_country}")

        return issues

    def _build_key(self, row: Dict[str, object], key_columns: List[str]) -> Tuple[str, ...]:
        return tuple(self._trim_value(row.get(col, "")) for col in key_columns)

    def _project_baseline(self, df: pd.DataFrame) -> pd.DataFrame:
        work = df.copy()
        for col in EXPECTED_COLUMNS_BASELINE:
            if col not in work.columns:
                work[col] = ""
        return work[EXPECTED_COLUMNS_BASELINE].copy()

    def _project_domo_raw_columns(self, domo_df: pd.DataFrame) -> List[str]:
        return [c for c in domo_df.columns if c in EXPECTED_COLUMNS_BASELINE]

    def _normalized_rows(self, df: pd.DataFrame) -> List[Tuple[str, ...]]:
        work = self._project_baseline(df).copy()
        for col in EXPECTED_COLUMNS_BASELINE:
            work[col] = self._trim_series(work[col])
        return list(map(tuple, work[EXPECTED_COLUMNS_BASELINE].to_records(index=False)))

    def _build_index(self, df: pd.DataFrame, key_columns: List[str]) -> Dict[Tuple[str, ...], Dict[str, object]]:
        available = [c for c in key_columns if c in df.columns]
        if len(available) != len(key_columns):
            return {}

        deduped = df.drop_duplicates(subset=key_columns, keep="first").copy()
        records = deduped.to_dict(orient="records")
        indexed: Dict[Tuple[str, ...], Dict[str, object]] = {}
        for row in records:
            indexed[self._build_key(row, key_columns)] = row
        return indexed

    def _to_decimal(self, value: object) -> Decimal | None:
        raw = self._trim_value(value)
        if not raw or raw.lower() in INVALID_TEXT_VALUES:
            return None
        try:
            return Decimal(raw)
        except (InvalidOperation, ValueError):
            return None

    def _normalize_timestamp_for_compare(self, value: object) -> str:
        raw = self._trim_value(value)
        if not raw:
            return ""

        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            return raw

        if TIMESTAMP_COMPARE_DATE_ONLY:
            return parsed.date().isoformat()

        return parsed.isoformat(sep=" ")

    def _normalize_timestamp_series_for_grouping(self, series: pd.Series) -> pd.Series:
        trimmed = self._trim_series(series)
        if not TIMESTAMP_COMPARE_DATE_ONLY:
            return trimmed

        parsed = pd.to_datetime(trimmed.replace("", pd.NA), errors="coerce")
        normalized = trimmed.copy()
        valid_mask = parsed.notna()

        if valid_mask.any():
            normalized.loc[valid_mask] = parsed.loc[valid_mask].dt.date.astype(str)

        return normalized

    def _normalize_timestamp_series_for_date_only(self, series: pd.Series) -> pd.Series:
        trimmed = self._trim_series(series)
        parsed = pd.to_datetime(trimmed.replace("", pd.NA), errors="coerce")

        normalized = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        valid_mask = parsed.notna()

        if valid_mask.any():
            normalized.loc[valid_mask] = parsed.loc[valid_mask].dt.normalize()

        return normalized

    def _normalize_date_like_value(self, value: object) -> str:
        raw = self._trim_value(value)
        if not raw:
            return ""

        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            return raw

        return parsed.date().isoformat()

    def _build_transition_signature(self, row: Dict[str, object]) -> Tuple[str, ...]:
        return (
            self._trim_value(row.get("Membership_Number", "")),
            self._trim_value(row.get("Account_Number_Full", "")),
            self._normalize_date_like_value(row.get("effectiveDate", "")),
            self._normalize_timestamp_for_compare(row.get(TIMESTAMP_COLUMN, "")),
            self._trim_value(row.get("membershipTypeCode", "")),
            self._trim_value(row.get("isPlatinumMember", "")),
        )

    def _normalize_potential_accruals_for_compare(self, value: object, source: str) -> str:
        raw = self._trim_value(value)
        if not raw:
            return ""

        parsed = self._to_decimal(raw)
        if parsed is None:
            return raw

        if source.upper() == "DOMO":
            return str(parsed.quantize(Decimal("0.01")))

        return str(parsed)

    def _normalize_value_for_compare(self, col: str, value: object, source: str) -> str:
        if col == TIMESTAMP_COLUMN:
            return self._normalize_timestamp_for_compare(value)

        if col == "Potential_Accruals":
            return self._normalize_potential_accruals_for_compare(value, source)

        return self._trim_value(value)

    def _is_nonzero_numeric(self, value: object) -> bool:
        parsed = self._to_decimal(value)
        return parsed is not None and parsed != Decimal("0")

    def _common_primary_keys(
        self,
        domo_df: pd.DataFrame,
        inf_df: pd.DataFrame,
    ) -> List[Tuple[str, ...]]:
        domo_index = self._build_index(domo_df, KEY_COLUMNS_PRIMARY)
        inf_index = self._build_index(inf_df, KEY_COLUMNS_PRIMARY)
        return sorted(set(domo_index.keys()) & set(inf_index.keys()))

    def _is_comparable_pair(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> bool:
        common_keys = self._common_primary_keys(domo_df, inf_df)
        if len(common_keys) < MIN_COMMON_PRIMARY_KEYS_FOR_STRICT_COMPARISON:
            return False

        if BOTH_FILES_ARE_CONSOLIDATED:
            return True

        if DOMO_IS_CONSOLIDATED and not self._same_snapshot():
            return False

        return True

    def _compare_rows_on_common_primary_keys(
        self,
        domo_df: pd.DataFrame,
        inf_df: pd.DataFrame,
    ) -> Tuple[int, int]:
        domo_index = self._build_index(domo_df, KEY_COLUMNS_PRIMARY)
        inf_index = self._build_index(inf_df, KEY_COLUMNS_PRIMARY)

        common_keys = sorted(set(domo_index.keys()) & set(inf_index.keys()))
        mismatch_count = 0
        mismatch_records: List[Dict[str, object]] = []

        for key in common_keys:
            domo_row = domo_index[key]
            inf_row = inf_index[key]

            different_fields: List[str] = []

            for col in EXPECTED_COLUMNS_BASELINE:
                domo_val = self._normalize_value_for_compare(col, domo_row.get(col, ""), "DOMO")
                inf_val = self._normalize_value_for_compare(col, inf_row.get(col, ""), "INFA")
                if domo_val != inf_val:
                    different_fields.append(col)

            if different_fields:
                mismatch_count += 1
                if len(mismatch_records) < MAX_EVIDENCE_ROWS:
                    mismatch_records.append(
                        {
                            "Membership_Number": key[0] if len(key) > 0 else "",
                            "Account_Number_Full": key[1] if len(key) > 1 else "",
                            "different_fields": ", ".join(different_fields[:50]),
                        }
                    )

        if mismatch_records:
            self.evidence["memberships_delta_common_key_row_diffs"] = pd.DataFrame(mismatch_records)

        return len(common_keys), mismatch_count

    def validate(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> ValidationRun:
        self._validate_snapshot()
        self._validate_schema(domo_df, inf_df)
        self._validate_empty_files(domo_df, inf_df)
        self._validate_special_characters(inf_df)
        self._validate_row_count(domo_df, inf_df)
        self._validate_keys(domo_df, inf_df)
        self._validate_required_not_blank(inf_df)
        self._validate_dates_and_numbers(inf_df)
        self._validate_country_iso3(inf_df)
        self._validate_country_scope(domo_df, inf_df)
        self._validate_account_card_consistency(inf_df)
        self._validate_household_value_vs_domo(domo_df, inf_df)
        self._validate_household_consistency(inf_df)
        self._validate_primary_consistency(inf_df)
        self._validate_inactive_account_consistency(inf_df)
        self._validate_allowed_status_values(inf_df)
        self._validate_expired_date_with_account_status(inf_df)
        self._validate_cardcount_vs_active_cards(inf_df)
        self._validate_timestamp(domo_df, inf_df)
        self._validate_delta_window(domo_df, inf_df)
        self._validate_cadence_support(domo_df, inf_df)
        self._validate_common_key_rows(domo_df, inf_df)
        self._validate_transition_presence_and_values(domo_df, inf_df)

        if not DELTA_SKIP_FULL_MULTISET_RECONCILIATION:
            self._validate_full_rows(domo_df, inf_df)

        self._validate_overall()

        summary = self._build_summary(domo_df, inf_df)
        return ValidationRun(summary=summary, checks=self.checks, dataframes=self.evidence)

    def _validate_snapshot(self) -> None:
        domo_snapshot_raw = self.snapshot_context.domo_snapshot.strip()
        inf_snapshot_raw = self.snapshot_context.informatica_snapshot.strip()

        if not domo_snapshot_raw and not inf_snapshot_raw:
            self.add_check(
                "MEM-DELTA-001",
                "Verify Delta snapshot values are documented",
                "WARNING",
                "File Control",
                field="Snapshot / comparison window",
                expected="Snapshot values are recommended",
                actual="No snapshot values were provided",
            )
            return

        if BOTH_FILES_ARE_CONSOLIDATED:
            status = "PASS" if self._snapshots_within_tolerance() else "FAIL"
            details = (
                f"Consolidated files allow up to {CONSOLIDATED_SNAPSHOT_TOLERANCE_MINUTES} minutes difference between documented snapshot values."
            )
        else:
            status = "PASS" if domo_snapshot_raw == inf_snapshot_raw else "FAIL"
            details = ""

        self.add_check(
            "MEM-DELTA-001",
            "Verify Delta snapshot values are documented",
            status,
            "File Control",
            field="Snapshot / comparison window",
            expected=domo_snapshot_raw or "Value expected",
            actual=inf_snapshot_raw or "Missing value",
            details=details,
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

        extra_domo = [
            c for c in domo_cols
            if c not in EXPECTED_COLUMNS_BASELINE and c not in ALLOWED_DOMO_EXTRA_COLUMNS
        ]
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
            evidence_file=(
                "memberships_delta_missing_columns_infa.csv"
                if missing_inf
                else ("memberships_delta_missing_columns_domo.csv" if missing_domo else "")
            ),
        )

        self.add_check(
            "MEM-DELTA-002C",
            "Verify Delta does not contain unexpected columns",
            "PASS" if not extra_domo and not extra_inf else "FAIL",
            "Schema",
            field="All columns",
            expected="No unexpected extra columns",
            actual=f"Unexpected extra in Domo={len(extra_domo)} | Extra in Informatica={len(extra_inf)}",
            details="Allowed Domo technical columns are ignored during schema validation.",
            evidence_file=(
                "memberships_delta_extra_columns_infa.csv"
                if extra_inf
                else ("memberships_delta_extra_columns_domo.csv" if extra_domo else "")
            ),
        )

        domo_projected_order = self._project_domo_raw_columns(domo_df)
        inf_projected_order = [c for c in inf_cols if c in EXPECTED_COLUMNS_BASELINE]

        self.add_check(
            "MEM-DELTA-002D",
            "Verify Delta preserves exact raw column order",
            "PASS"
            if domo_projected_order == EXPECTED_COLUMNS_BASELINE and inf_projected_order == EXPECTED_COLUMNS_BASELINE
            else "FAIL",
            "Schema",
            field="All columns",
            expected="Exact raw baseline order",
            actual=(
                f"Domo raw order ok={domo_projected_order == EXPECTED_COLUMNS_BASELINE} | "
                f"Informatica raw order ok={inf_projected_order == EXPECTED_COLUMNS_BASELINE}"
            ),
            details="Domo technical enrichment columns are ignored for the order check.",
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

    def _validate_special_characters(self, inf_df: pd.DataFrame) -> None:
        bad_records: List[Dict[str, object]] = []
        count = 0

        for col in inf_df.columns:
            series = self._trim_series(inf_df[col])
            mask = pd.Series(False, index=series.index)
            for special_char in DISALLOWED_SPECIAL_CHARACTERS:
                escaped = special_char.replace("\\", r"\\")
                mask = mask | series.str.contains(escaped, regex=True, na=False)

            if mask.any():
                for idx in inf_df.index[mask][:MAX_EVIDENCE_ROWS]:
                    count += 1
                    if len(bad_records) < MAX_EVIDENCE_ROWS:
                        bad_records.append(
                            {
                                "row_index": idx,
                                "column": col,
                                "value": self._trim_value(inf_df.at[idx, col]),
                            }
                        )

        if bad_records:
            self.evidence["memberships_delta_backslash_issues"] = pd.DataFrame(bad_records)

        self.add_check(
            "MEM-DELTA-003A",
            'Special Character Validation "\\"',
            "PASS" if count == 0 else "FAIL",
            "Format Validation",
            field="All columns",
            expected='No backslash "\\" character allowed anywhere in the file',
            actual=f"Rows/fields with backslash={count}",
            evidence_file="memberships_delta_backslash_issues.csv" if count > 0 else "",
        )

    def _validate_row_count(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        same_count = len(domo_df) == len(inf_df)

        if same_count:
            status = "PASS"
        elif BOTH_FILES_ARE_CONSOLIDATED:
            status = "FAIL"
        elif DOMO_IS_CONSOLIDATED:
            status = "WARNING" if DELTA_ALLOW_ROW_COUNT_DIFFERENCE else "FAIL"
        else:
            status = "FAIL" if self._same_snapshot() else ("WARNING" if DELTA_ALLOW_ROW_COUNT_DIFFERENCE else "FAIL")

        details = (
            "Both files are consolidated, so row count must match exactly."
            if BOTH_FILES_ARE_CONSOLIDATED
            else (
                f"Domo is treated as a consolidated file spanning multiple 5-minute generations across up to {DOMO_CONSOLIDATED_WINDOW_MINUTES} minutes."
                if DOMO_IS_CONSOLIDATED
                else "Exact record count is required when both files belong to the same snapshot. If snapshots differ, this remains informational in Delta mode."
            )
        )

        self.add_check(
            "MEM-DELTA-004",
            "Validate record count consistency between Informatica and Domo",
            status,
            "Volume",
            field="All records",
            expected=str(len(domo_df)),
            actual=str(len(inf_df)),
            details=details,
        )

    def _validate_keys(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        for idx, col in enumerate(KEY_COLUMNS_PRIMARY + KEY_COLUMNS_SECONDARY, start=1):
            check_id = f"MEM-DELTA-005{idx}"

            if col not in domo_df.columns or col not in inf_df.columns:
                self.add_check(
                    check_id,
                    f"Validate {col} population and formatting",
                    "FAIL",
                    "Population / Datatype",
                    field=col,
                    expected="Column exists in both files",
                    actual="Missing in one or both files",
                )
                continue

            domo_vals = set(self._trim_series(domo_df[col]))
            inf_vals = set(self._trim_series(inf_df[col]))
            only_domo = sorted(domo_vals - inf_vals)
            only_inf = sorted(inf_vals - domo_vals)

            bad_format = 0
            for value in inf_vals:
                if not value:
                    continue
                if col in {"Membership_Number", "Account_Number_Full", "member_id", "cardId"}:
                    if not value.replace("-", "").isalnum():
                        bad_format += 1

            if only_domo or only_inf:
                self.evidence[f"memberships_delta_key_diff_{col}"] = pd.DataFrame(
                    [{"source": "DOMO_ONLY", col: x} for x in only_domo[:MAX_EVIDENCE_ROWS]]
                    + [{"source": "INFA_ONLY", col: x} for x in only_inf[:MAX_EVIDENCE_ROWS]]
                )

            same_set = not only_domo and not only_inf
            if bad_format > 0:
                status = "FAIL"
            elif same_set:
                status = "PASS"
            elif BOTH_FILES_ARE_CONSOLIDATED:
                status = "FAIL"
            elif DOMO_IS_CONSOLIDATED:
                status = "WARNING" if DELTA_ALLOW_KEY_SET_DIFFERENCE else "FAIL"
            else:
                status = "FAIL" if self._same_snapshot() else ("WARNING" if DELTA_ALLOW_KEY_SET_DIFFERENCE else "FAIL")

            self.add_check(
                check_id,
                f"Validate {col} population and formatting",
                status,
                "Population / Datatype",
                field=col,
                expected="Valid format and exact set equality for the same comparable snapshot",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)} | bad_format={bad_format}",
                details=(
                    "Both files are consolidated, so key sets must match exactly."
                    if BOTH_FILES_ARE_CONSOLIDATED and not same_set
                    else (
                        f"Domo is treated as a consolidated file spanning multiple 5-minute generations across up to {DOMO_CONSOLIDATED_WINDOW_MINUTES} minutes."
                        if DOMO_IS_CONSOLIDATED and not same_set
                        else "In Delta mode, set differences downgrade to warning when the files are not from the same documented snapshot."
                    )
                ),
                evidence_file=f"memberships_delta_key_diff_{col}.csv" if only_domo or only_inf else "",
            )

        card_check_id = f"MEM-DELTA-005{len(KEY_COLUMNS_PRIMARY + KEY_COLUMNS_SECONDARY) + 1}"
        if all(col in domo_df.columns for col in CARD_KEY_COLUMNS) and all(col in inf_df.columns for col in CARD_KEY_COLUMNS):
            domo_card_keys = {self._build_key(row, CARD_KEY_COLUMNS) for row in domo_df.to_dict(orient="records")}
            inf_card_keys = {self._build_key(row, CARD_KEY_COLUMNS) for row in inf_df.to_dict(orient="records")}

            only_domo = sorted(domo_card_keys - inf_card_keys)
            only_inf = sorted(inf_card_keys - domo_card_keys)

            if only_domo or only_inf:
                self.evidence["memberships_delta_key_diff_membership_card"] = pd.DataFrame(
                    [
                        {
                            "source": "DOMO_ONLY",
                            CARD_KEY_COLUMNS[0]: key[0] if len(key) > 0 else "",
                            CARD_KEY_COLUMNS[1]: key[1] if len(key) > 1 else "",
                        }
                        for key in only_domo[:MAX_EVIDENCE_ROWS]
                    ]
                    + [
                        {
                            "source": "INFA_ONLY",
                            CARD_KEY_COLUMNS[0]: key[0] if len(key) > 0 else "",
                            CARD_KEY_COLUMNS[1]: key[1] if len(key) > 1 else "",
                        }
                        for key in only_inf[:MAX_EVIDENCE_ROWS]
                    ]
                )

            same_set = not only_domo and not only_inf
            if same_set:
                status = "PASS"
            elif BOTH_FILES_ARE_CONSOLIDATED:
                status = "FAIL"
            elif DOMO_IS_CONSOLIDATED:
                status = "WARNING" if DELTA_ALLOW_KEY_SET_DIFFERENCE else "FAIL"
            else:
                status = "FAIL" if self._same_snapshot() else ("WARNING" if DELTA_ALLOW_KEY_SET_DIFFERENCE else "FAIL")

            self.add_check(
                card_check_id,
                "Validate Membership_Number and cardId populations",
                status,
                "Population / Datatype",
                field=", ".join(CARD_KEY_COLUMNS),
                expected="Exact set equality for Membership_Number + cardId in the same comparable snapshot",
                actual=f"only_in_domo={len(only_domo)} | only_in_infa={len(only_inf)}",
                details=(
                    "Both files are consolidated, so Membership_Number + cardId must match exactly."
                    if BOTH_FILES_ARE_CONSOLIDATED and not same_set
                    else (
                        f"Domo is treated as a consolidated file spanning multiple 5-minute generations across up to {DOMO_CONSOLIDATED_WINDOW_MINUTES} minutes."
                        if DOMO_IS_CONSOLIDATED and not same_set
                        else "In Delta mode, differences downgrade to warning when snapshots differ."
                    )
                ),
                evidence_file="memberships_delta_key_diff_membership_card.csv" if only_domo or only_inf else "",
            )
        else:
            self.add_check(
                card_check_id,
                "Validate Membership_Number and cardId populations",
                "FAIL",
                "Population / Datatype",
                field=", ".join(CARD_KEY_COLUMNS),
                expected="Both composite key columns exist",
                actual="Missing in one or both files",
            )

    def _validate_required_not_blank(self, inf_df: pd.DataFrame) -> None:
        for col in REQUIRED_NOT_BLANK_FIELDS:
            if col not in inf_df.columns:
                self.add_check(
                    f"MEM-DELTA-007-{col}",
                    f"Not blank or null fields in {col}",
                    "FAIL",
                    "Mandatory Fields",
                    field=col,
                    expected="Column exists",
                    actual="Column missing",
                )
                continue

            raw = inf_df[col]
            trimmed = self._trim_series(raw)
            issues = (
                int(raw.isna().sum())
                + int(trimmed.eq("").sum())
                + int(trimmed.str.lower().isin(INVALID_TEXT_VALUES).sum())
            )

            self.add_check(
                f"MEM-DELTA-007-{col}",
                f"Not blank or null fields in {col}",
                "PASS" if issues == 0 else "FAIL",
                "Mandatory Fields",
                field=col,
                expected="No nulls, blanks, or invalid textual null markers",
                actual=f"Issues found={issues}",
            )

    def _validate_dates_and_numbers(self, inf_df: pd.DataFrame) -> None:
        for col in DATE_COLUMNS:
            if col not in inf_df.columns:
                self.add_check(
                    f"MEM-DELTA-008-DATE-{col}",
                    f"Validate date field format for {col}",
                    "FAIL",
                    "Format Validation",
                    field=col,
                    expected="Column exists",
                    actual="Column missing",
                )
                continue

            trimmed = self._trim_series(inf_df[col])
            parsed = pd.to_datetime(trimmed.replace("", pd.NA), errors="coerce")
            invalid = int(parsed.isna().sum()) - int(trimmed.eq("").sum())

            self.add_check(
                f"MEM-DELTA-008-DATE-{col}",
                f"Validate date field format for {col}",
                "PASS" if invalid == 0 else "FAIL",
                "Format Validation",
                field=col,
                expected="Parseable date values",
                actual=f"Invalid values={invalid}",
            )

        for col in NUMERIC_COLUMNS:
            if col not in inf_df.columns:
                self.add_check(
                    f"MEM-DELTA-008-NUM-{col}",
                    f"Validate numeric field format for {col}",
                    "FAIL",
                    "Format Validation",
                    field=col,
                    expected="Column exists",
                    actual="Column missing",
                )
                continue

            invalid = 0
            for value in self._trim_series(inf_df[col]).tolist():
                if value == "":
                    continue
                parsed = self._to_decimal(value)
                if parsed is None:
                    invalid += 1
                elif col in INTEGER_COLUMNS and parsed != parsed.to_integral_value():
                    invalid += 1

            self.add_check(
                f"MEM-DELTA-008-NUM-{col}",
                f"Validate numeric field format for {col}",
                "PASS" if invalid == 0 else "FAIL",
                "Format Validation",
                field=col,
                expected="Valid approved numeric structure",
                actual=f"Invalid values={invalid}",
            )

        for col in BOOLEAN_LIKE_COLUMNS:
            if col not in inf_df.columns:
                self.add_check(
                    f"MEM-DELTA-008-BOOL-{col}",
                    f"Verify boolean-like behavior for {col}",
                    "FAIL",
                    "Format Validation",
                    field=col,
                    expected="Column exists",
                    actual="Column missing",
                )
                continue

            vals = set(self._trim_series(inf_df[col]).str.upper())
            if col == "primary":
                allowed = PRIMARY_ALLOWED_VALUES
            elif col == "isPlatinumMember":
                allowed = PLATINUM_ALLOWED_VALUES
            else:
                allowed = {"Y", "N", "YES", "NO", "TRUE", "FALSE", "0", "1", ""}

            invalid = sorted(vals - allowed)

            self.add_check(
                f"MEM-DELTA-008-BOOL-{col}",
                f"Verify boolean-like behavior for {col}",
                "PASS" if not invalid else "FAIL",
                "Format Validation",
                field=col,
                expected="Allowed boolean-like values",
                actual=", ".join(invalid[:10]) if invalid else "No invalid values",
            )

    def _validate_country_iso3(self, inf_df: pd.DataFrame) -> None:
        if "Country_Code_Full" not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-008-ISO3",
                "Validate Country_Code_Full ISO3 values",
                "FAIL",
                "Country Validation",
                field="Country_Code_Full",
                expected="Column exists",
                actual="Column missing",
            )
            return

        invalid_records: List[Dict[str, object]] = []
        invalid = 0
        for row in inf_df.to_dict(orient="records"):
            raw = self._trim_value(row.get("Country_Code_Full", "")).upper()
            if raw and self._normalize_country_iso3(raw) == "":
                invalid += 1
                if len(invalid_records) < MAX_EVIDENCE_ROWS:
                    invalid_records.append(
                        {
                            "Membership_Number": row.get("Membership_Number", ""),
                            "Account_Number_Full": row.get("Account_Number_Full", ""),
                            "Country_Code_Full": raw,
                        }
                    )

        if invalid_records:
            self.evidence["memberships_delta_invalid_country_iso3"] = pd.DataFrame(invalid_records)

        self.add_check(
            "MEM-DELTA-008-ISO3",
            "Validate Country_Code_Full ISO3 values",
            "PASS" if invalid == 0 else "FAIL",
            "Country Validation",
            field="Country_Code_Full",
            expected="Allowed ISO3 country codes only",
            actual=f"Invalid ISO3 values={invalid}",
            evidence_file="memberships_delta_invalid_country_iso3.csv" if invalid > 0 else "",
        )

    def _validate_country_scope(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        missing_country_cols = [c for c in COUNTRY_COLUMNS if c not in domo_df.columns or c not in inf_df.columns]
        if missing_country_cols:
            self.add_check(
                "MEM-DELTA-006A",
                "Verify strict Delta country validation prerequisites exist",
                "FAIL",
                "Cross-Field",
                field="; ".join(COUNTRY_COLUMNS),
                expected="All country columns exist in both files",
                actual=f"Missing columns={len(missing_country_cols)}",
                details=", ".join(missing_country_cols),
            )
            return

        domo_indexed = self._build_index(domo_df, KEY_COLUMNS_PRIMARY)
        inf_indexed = self._build_index(inf_df, KEY_COLUMNS_PRIMARY)

        field_diff_records: List[Dict[str, object]] = []
        internal_alignment_issues: List[Dict[str, object]] = []
        country_count_diffs: List[Dict[str, object]] = []
        invalid_country_values: List[Dict[str, object]] = []

        for country in sorted(COUNTRY_ALPHA2_CODES):
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

            if count_match:
                count_status = "PASS"
            elif BOTH_FILES_ARE_CONSOLIDATED:
                count_status = "FAIL"
            elif DOMO_IS_CONSOLIDATED:
                count_status = "WARNING" if DELTA_ALLOW_COUNTRY_COUNT_DIFFERENCE else "FAIL"
            else:
                count_status = "FAIL" if self._same_snapshot() else ("WARNING" if DELTA_ALLOW_COUNTRY_COUNT_DIFFERENCE else "FAIL")

            self.add_check(
                f"MEM-DELTA-{country}-COUNT",
                f"Validate country universe count for {country}",
                count_status,
                "Cross-Field",
                country=country,
                field="Country universe",
                expected=f"Domo count={len(domo_country_keys)}",
                actual=f"Informatica count={len(inf_country_keys)}",
                details=(
                    "Both files are consolidated, so country counts must match exactly."
                    if BOTH_FILES_ARE_CONSOLIDATED and not count_match
                    else (
                        f"Domo is treated as a consolidated file spanning multiple 5-minute generations across up to {DOMO_CONSOLIDATED_WINDOW_MINUTES} minutes."
                        if DOMO_IS_CONSOLIDATED and not count_match
                        else "Country volume differences are treated as blocking only when the files belong to the same documented snapshot."
                    )
                ),
                evidence_file="memberships_delta_country_count_diffs.csv" if not count_match else "",
            )

            common_country_keys = sorted(domo_country_keys & inf_country_keys)

            for country_col in COUNTRY_COLUMNS:
                mismatch_count = 0
                invalid_count = 0

                for key in common_country_keys:
                    domo_row = domo_indexed[key]
                    inf_row = inf_indexed[key]

                    if country_col == "Country_Code_Full":
                        domo_norm = self._normalize_country_iso3(domo_row.get(country_col, ""))
                        inf_norm = self._normalize_country_iso3(inf_row.get(country_col, ""))
                    else:
                        domo_norm = self._normalize_country_alpha2(domo_row.get(country_col, ""))
                        inf_norm = self._normalize_country_alpha2(inf_row.get(country_col, ""))

                    if domo_norm != inf_norm:
                        mismatch_count += 1
                        if len(field_diff_records) < MAX_EVIDENCE_ROWS:
                            field_diff_records.append(
                                {
                                    "Membership_Number": key[0] if len(key) > 0 else "",
                                    "Account_Number_Full": key[1] if len(key) > 1 else "",
                                    "country": country,
                                    "field": country_col,
                                    "domo_raw": domo_row.get(country_col, ""),
                                    "informatica_raw": inf_row.get(country_col, ""),
                                    "domo_normalized": domo_norm,
                                    "informatica_normalized": inf_norm,
                                }
                            )

                    raw_inf = self._trim_value(inf_row.get(country_col, ""))
                    if country_col == "Country_Code_Full":
                        invalid_condition = raw_inf != "" and self._normalize_country_iso3(raw_inf) == ""
                    else:
                        invalid_condition = raw_inf != "" and self._normalize_country_alpha2(raw_inf) == ""

                    if invalid_condition:
                        invalid_count += 1
                        if len(invalid_country_values) < MAX_EVIDENCE_ROWS:
                            invalid_country_values.append(
                                {
                                    "Membership_Number": key[0] if len(key) > 0 else "",
                                    "Account_Number_Full": key[1] if len(key) > 1 else "",
                                    "country": country,
                                    "field": country_col,
                                    "raw_value": inf_row.get(country_col, ""),
                                }
                            )

                self.add_check(
                    f"MEM-DELTA-{country}-{country_col}",
                    f"Verify that {country_col} is preserved exactly for {country}",
                    "PASS" if mismatch_count == 0 else "FAIL",
                    "Cross-Field",
                    country=country,
                    field=country_col,
                    expected="Exact normalized country equivalence for matching membership keys",
                    actual="Matched" if mismatch_count == 0 else f"record_differences={mismatch_count}",
                    details=(
                        f"{invalid_count} invalid normalized values detected in Informatica."
                        if invalid_count > 0
                        else "Country output is evaluated with alpha2 alignment and Country_Code_Full ISO3 support."
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
                                "country_code": inf_row.get("country_code", ""),
                                "Country_Code_Full": inf_row.get("Country_Code_Full", ""),
                                "club_code": inf_row.get("club_code", ""),
                            }
                        )

            self.add_check(
                f"MEM-DELTA-{country}-ALIGN",
                f"Validate country and club alignment for {country}",
                "PASS" if alignment_failures == 0 else "FAIL",
                "Cross-Field",
                country=country,
                field="country_code; Country_Code_Full; club_code",
                expected="country_code, Country_Code_Full ISO3, and club_code must align to the same country",
                actual="Matched" if alignment_failures == 0 else f"row_alignment_failures={alignment_failures}",
                details="Club alignment uses the approved club-country map configured in the validator.",
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
        overall_invalid_records: List[Dict[str, object]] = []

        for row in inf_df.to_dict(orient="records"):
            for col in COUNTRY_COLUMNS:
                raw = row.get(col, "")
                raw_trimmed = self._trim_value(raw)
                if raw_trimmed == "":
                    continue

                if col == "Country_Code_Full":
                    valid = self._normalize_country_iso3(raw_trimmed) != ""
                else:
                    valid = self._normalize_country_alpha2(raw_trimmed) != ""

                if not valid:
                    overall_validity_issues += 1
                    if len(overall_invalid_records) < MAX_EVIDENCE_ROWS:
                        overall_invalid_records.append(
                            {
                                "Membership_Number": row.get("Membership_Number", ""),
                                "Account_Number_Full": row.get("Account_Number_Full", ""),
                                "field": col,
                                "raw_value": raw,
                            }
                        )

        if overall_invalid_records:
            self.evidence["memberships_delta_invalid_country_values"] = pd.DataFrame(overall_invalid_records)

        self.add_check(
            "MEM-DELTA-006Z",
            "Verify all Delta country fields contain recognized country values",
            "PASS" if overall_validity_issues == 0 else "FAIL",
            "Cross-Field",
            field="; ".join(COUNTRY_COLUMNS),
            expected="All populated country fields resolve to approved country siglas / ISO3 values",
            actual="No invalid values" if overall_validity_issues == 0 else f"invalid_values={overall_validity_issues}",
            evidence_file="memberships_delta_invalid_country_values.csv" if overall_validity_issues > 0 else "",
        )

    def _validate_account_card_consistency(self, inf_df: pd.DataFrame) -> None:
        if "Account_Number_Full" not in inf_df.columns or "cardId" not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-013",
                "Validate Account_Number_Full and cardId consistency",
                "FAIL",
                "Business Rule",
                field="Account_Number_Full; cardId",
                expected="Both columns exist",
                actual="Missing in Informatica",
            )
            return

        issues: List[Dict[str, object]] = []
        count = 0
        for row in inf_df.to_dict(orient="records"):
            account = self._trim_value(row.get("Account_Number_Full", ""))
            card_id = self._trim_value(row.get("cardId", ""))
            if not account or not card_id:
                continue
            suffix = account.split("-")[-1] if "-" in account else account[-len(card_id):]
            if suffix != card_id:
                count += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    issues.append(
                        {
                            "Membership_Number": row.get("Membership_Number", ""),
                            "Account_Number_Full": account,
                            "cardId": card_id,
                            "derived_suffix": suffix,
                        }
                    )

        if issues:
            self.evidence["memberships_delta_account_card_consistency"] = pd.DataFrame(issues)

        self.add_check(
            "MEM-DELTA-013",
            "Validate Account_Number_Full and cardId consistency",
            "PASS" if count == 0 else "FAIL",
            "Business Rule",
            field="Account_Number_Full; cardId",
            expected="cardId must match the trailing card segment of Account_Number_Full",
            actual=f"Inconsistent rows={count}",
            evidence_file="memberships_delta_account_card_consistency.csv" if count > 0 else "",
        )

    def _validate_household_value_vs_domo(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        key = "Membership_Number"
        value_col = "Household_Accruing_Balance_Acumulated"

        if key not in domo_df.columns or key not in inf_df.columns or value_col not in domo_df.columns or value_col not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-014",
                "Validate value reported for Household_Accruing_Balance_Acumulated by Membership_Number",
                "FAIL",
                "Business Rule",
                field=f"{key}; {value_col}",
                expected="Columns exist in both files",
                actual="Missing columns",
            )
            return

        if not self._is_comparable_pair(domo_df, inf_df):
            status = "WARNING" if ALLOW_WARNING_WHEN_NOT_COMPARABLE else "FAIL"
            self.add_check(
                "MEM-DELTA-014",
                "Validate value reported for Household_Accruing_Balance_Acumulated by Membership_Number",
                status,
                "Business Rule",
                field=f"{key}; {value_col}",
                expected="Comparable Domo and Informatica files with overlapping memberships from the same effective window",
                actual="Files are not comparable for strict cross-file value validation",
                details="Comparable overlap is required for strict cross-file value validation.",
            )
            return

        domo_map = (
            domo_df[[key, value_col]]
            .drop_duplicates(subset=[key], keep="first")
            .assign(**{key: lambda x: self._trim_series(x[key]), value_col: lambda x: self._trim_series(x[value_col])})
            .set_index(key)[value_col]
            .to_dict()
        )
        inf_map = (
            inf_df[[key, value_col]]
            .drop_duplicates(subset=[key], keep="first")
            .assign(**{key: lambda x: self._trim_series(x[key]), value_col: lambda x: self._trim_series(x[value_col])})
            .set_index(key)[value_col]
            .to_dict()
        )

        common = sorted(set(domo_map) & set(inf_map))
        issues = []
        mismatch = 0
        for membership in common:
            if domo_map[membership] != inf_map[membership]:
                mismatch += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    issues.append(
                        {
                            "Membership_Number": membership,
                            "domo_value": domo_map[membership],
                            "informatica_value": inf_map[membership],
                        }
                    )

        if issues:
            self.evidence["memberships_delta_household_vs_domo"] = pd.DataFrame(issues)

        self.add_check(
            "MEM-DELTA-014",
            "Validate value reported for Household_Accruing_Balance_Acumulated by Membership_Number",
            "PASS" if mismatch == 0 else "FAIL",
            "Business Rule",
            field=f"{key}; {value_col}",
            expected="Exact value equality by Membership_Number for overlapping memberships",
            actual=f"Overlapping memberships={len(common)} | mismatches={mismatch}",
            evidence_file="memberships_delta_household_vs_domo.csv" if mismatch > 0 else "",
        )

    def _validate_household_consistency(self, inf_df: pd.DataFrame) -> None:
        required = {"Membership_Number", "Household_Accruing_Balance_Acumulated"}
        if not required.issubset(set(inf_df.columns)):
            self.add_check(
                "MEM-DELTA-015",
                "Validate Household Accruing Balance consistency",
                "FAIL",
                "Business Rule",
                field="Membership_Number; Household_Accruing_Balance_Acumulated",
                expected="Columns exist",
                actual="Missing columns",
            )
            return

        work = inf_df.copy()
        work["Membership_Number"] = self._trim_series(work["Membership_Number"])
        work["Household_Accruing_Balance_Acumulated"] = self._trim_series(
            work["Household_Accruing_Balance_Acumulated"]
        )

        group_columns = ["Membership_Number"]
        uses_snapshot_grouping = False

        if BOTH_FILES_ARE_CONSOLIDATED and TIMESTAMP_COLUMN in work.columns:
            work[TIMESTAMP_COLUMN] = self._normalize_timestamp_series_for_grouping(work[TIMESTAMP_COLUMN])
            group_columns = ["Membership_Number", TIMESTAMP_COLUMN]
            uses_snapshot_grouping = True

        issues: List[Dict[str, object]] = []
        inconsistent = 0

        for group_key, group in work.groupby(group_columns):
            membership = group_key[0] if isinstance(group_key, tuple) else group_key
            snapshot_value = group_key[1] if isinstance(group_key, tuple) and len(group_key) > 1 else ""

            vals = {
                v for v in group["Household_Accruing_Balance_Acumulated"].tolist()
                if v != ""
            }

            if len(vals) > 1:
                inconsistent += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    record = {
                        "Membership_Number": membership,
                        "observed_values": ", ".join(sorted(vals)),
                    }
                    if uses_snapshot_grouping:
                        record[TIMESTAMP_COLUMN] = snapshot_value
                    issues.append(record)

        if issues:
            self.evidence["memberships_delta_household_internal_consistency"] = pd.DataFrame(issues)

        details = (
            "For consolidated files, this validation is executed by Membership_Number + Timestamp_Run to avoid false inconsistencies across different runs inside the same consolidated window."
            if uses_snapshot_grouping
            else "Validation is executed by Membership_Number."
        )

        self.add_check(
            "MEM-DELTA-015",
            "Validate Household Accruing Balance consistency",
            "PASS" if inconsistent == 0 else "FAIL",
            "Business Rule",
            field="Membership_Number; Household_Accruing_Balance_Acumulated",
            expected="A validation group must not have conflicting household balance values",
            actual=f"Inconsistent groups={inconsistent}",
            details=details,
            evidence_file="memberships_delta_household_internal_consistency.csv" if inconsistent > 0 else "",
        )

    def _validate_primary_consistency(self, inf_df: pd.DataFrame) -> None:
        required = {"Membership_Number", "Account_Number_Full", "primary", "cardStatusCode"}
        if not required.issubset(set(inf_df.columns)):
            self.add_check(
                "MEM-DELTA-016",
                "Validate primary consistency",
                "FAIL",
                "Business Rule",
                field="Membership_Number; Account_Number_Full; primary",
                expected="Required columns exist",
                actual="Missing columns",
            )
            return

        work = inf_df.copy()
        work["Membership_Number"] = self._trim_series(work["Membership_Number"])
        work["primary"] = self._trim_series(work["primary"]).str.upper()
        work["cardStatusCode"] = self._trim_series(work["cardStatusCode"])

        group_columns = ["Membership_Number"]
        uses_snapshot_grouping = False

        if BOTH_FILES_ARE_CONSOLIDATED and TIMESTAMP_COLUMN in work.columns:
            work[TIMESTAMP_COLUMN] = self._normalize_timestamp_series_for_grouping(work[TIMESTAMP_COLUMN])
            group_columns = ["Membership_Number", TIMESTAMP_COLUMN]
            uses_snapshot_grouping = True

        issues: List[Dict[str, object]] = []
        inconsistent = 0

        for group_key, group in work.groupby(group_columns):
            membership = group_key[0] if isinstance(group_key, tuple) else group_key
            snapshot_value = group_key[1] if isinstance(group_key, tuple) and len(group_key) > 1 else ""

            active_primary_count = len(
                group[(group["primary"] == "Y") & (group["cardStatusCode"].isin(ACTIVE_CARD_STATUS_CODES))]
            )

            if active_primary_count != 1:
                inconsistent += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    record = {
                        "Membership_Number": membership,
                        "active_primary_count": active_primary_count,
                    }
                    if uses_snapshot_grouping:
                        record[TIMESTAMP_COLUMN] = snapshot_value
                    issues.append(record)

        if issues:
            self.evidence["memberships_delta_primary_consistency"] = pd.DataFrame(issues)

        details = (
            "For consolidated files, this validation is executed by Membership_Number + Timestamp_Run to avoid false inconsistencies across different runs inside the same consolidated window."
            if uses_snapshot_grouping
            else "Validation is executed by Membership_Number."
        )

        self.add_check(
            "MEM-DELTA-016",
            "Validate primary consistency",
            "PASS" if inconsistent == 0 else "FAIL",
            "Business Rule",
            field="Membership_Number; Account_Number_Full; primary",
            expected="Exactly one active primary card per validation group",
            actual=f"Inconsistent groups={inconsistent}",
            details=details,
            evidence_file="memberships_delta_primary_consistency.csv" if inconsistent > 0 else "",
        )

    def _validate_inactive_account_consistency(self, inf_df: pd.DataFrame) -> None:
        required = {"Membership_Number", "accountStatusCode", "membershipTypeCode"}
        if not required.issubset(set(inf_df.columns)):
            self.add_check(
                "MEM-DELTA-017",
                "Validate inactive account consistency by Membership_Number",
                "FAIL",
                "Business Rule",
                field="Membership_Number; accountStatusCode; membershipTypeCode",
                expected="Required columns exist",
                actual="Missing columns",
            )
            return

        work = inf_df.copy()
        work["Membership_Number"] = self._trim_series(work["Membership_Number"])
        work["accountStatusCode"] = self._trim_series(work["accountStatusCode"])
        work["membershipTypeCode"] = self._trim_series(work["membershipTypeCode"])

        group_columns = ["Membership_Number"]
        uses_snapshot_grouping = False

        if BOTH_FILES_ARE_CONSOLIDATED and TIMESTAMP_COLUMN in work.columns:
            work[TIMESTAMP_COLUMN] = self._normalize_timestamp_series_for_grouping(work[TIMESTAMP_COLUMN])
            group_columns = ["Membership_Number", TIMESTAMP_COLUMN]
            uses_snapshot_grouping = True

        issues: List[Dict[str, object]] = []
        inconsistent = 0

        for group_key, group in work.groupby(group_columns):
            membership = group_key[0] if isinstance(group_key, tuple) else group_key
            snapshot_value = group_key[1] if isinstance(group_key, tuple) and len(group_key) > 1 else ""

            statuses = {v for v in group["accountStatusCode"].tolist() if v != ""}
            if len(statuses) > 1:
                inconsistent += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    record = {
                        "Membership_Number": membership,
                        "account_statuses": ", ".join(sorted(statuses)),
                    }
                    if uses_snapshot_grouping:
                        record[TIMESTAMP_COLUMN] = snapshot_value
                    issues.append(record)

        if issues:
            self.evidence["memberships_delta_inactive_account_consistency"] = pd.DataFrame(issues)

        details = (
            "For consolidated files, this validation is executed by Membership_Number + Timestamp_Run to avoid false inconsistencies across different runs inside the same consolidated window."
            if uses_snapshot_grouping
            else "Validation is executed by Membership_Number."
        )

        self.add_check(
            "MEM-DELTA-017",
            "Validate inactive account consistency by Membership_Number",
            "PASS" if inconsistent == 0 else "FAIL",
            "Business Rule",
            field="Membership_Number; accountStatusCode; membershipTypeCode",
            expected="A validation group should not carry conflicting accountStatusCode values",
            actual=f"Inconsistent groups={inconsistent}",
            details=details,
            evidence_file="memberships_delta_inactive_account_consistency.csv" if inconsistent > 0 else "",
        )

    def _validate_allowed_status_values(self, inf_df: pd.DataFrame) -> None:
        if "accountStatusCode" not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-018",
                "Validate allowed values for accountStatusCode",
                "FAIL",
                "Domain Validation",
                field="accountStatusCode",
                expected="Column exists",
                actual="Column missing",
            )
        else:
            vals = set(self._trim_series(inf_df["accountStatusCode"]))
            invalid = sorted(v for v in vals if v and v not in ACCOUNT_STATUS_ALLOWED)
            self.add_check(
                "MEM-DELTA-018",
                "Validate allowed values for accountStatusCode",
                "PASS" if not invalid else "FAIL",
                "Domain Validation",
                field="accountStatusCode",
                expected=", ".join(sorted(ACCOUNT_STATUS_ALLOWED)),
                actual=", ".join(invalid[:20]) if invalid else "No invalid values",
            )

        if "cardStatusCode" not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-019",
                "Validate allowed values for cardStatusCode",
                "FAIL",
                "Domain Validation",
                field="cardStatusCode",
                expected="Column exists",
                actual="Column missing",
            )
        else:
            vals = set(self._trim_series(inf_df["cardStatusCode"]))
            invalid = sorted(v for v in vals if v and v not in CARD_STATUS_ALLOWED)
            self.add_check(
                "MEM-DELTA-019",
                "Validate allowed values for cardStatusCode",
                "PASS" if not invalid else "FAIL",
                "Domain Validation",
                field="cardStatusCode",
                expected=", ".join(sorted(CARD_STATUS_ALLOWED)),
                actual=", ".join(invalid[:20]) if invalid else "No invalid values",
            )

    def _validate_expired_date_with_account_status(self, inf_df: pd.DataFrame) -> None:
        if "expiredDate" not in inf_df.columns or "accountStatusCode" not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-020",
                "Validate expiredDate with accountStatusCode",
                "FAIL",
                "Business Rule",
                field="expiredDate; accountStatusCode",
                expected="Both columns exist",
                actual="Missing columns",
            )
            return

        issues = []
        count = 0
        for row in inf_df.to_dict(orient="records"):
            status = self._trim_value(row.get("accountStatusCode", ""))
            expired = self._trim_value(row.get("expiredDate", ""))
            if status and status not in ACTIVE_ACCOUNT_STATUS_CODES and not expired:
                count += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    issues.append(
                        {
                            "Membership_Number": row.get("Membership_Number", ""),
                            "accountStatusCode": status,
                            "expiredDate": expired,
                        }
                    )

        if issues:
            self.evidence["memberships_delta_expired_date_status"] = pd.DataFrame(issues)

        self.add_check(
            "MEM-DELTA-020",
            "Validate expiredDate with accountStatusCode",
            "PASS" if count == 0 else "FAIL",
            "Business Rule",
            field="expiredDate; accountStatusCode",
            expected="Inactive account statuses must carry expiredDate",
            actual=f"Rows failing rule={count}",
            evidence_file="memberships_delta_expired_date_status.csv" if count > 0 else "",
        )

    def _validate_cardcount_vs_active_cards(self, inf_df: pd.DataFrame) -> None:
        required = {"cardCount", "cardStatusCode", "Membership_Number"}
        if not required.issubset(set(inf_df.columns)):
            self.add_check(
                "MEM-DELTA-021",
                "Validate cardCount correctly displays the number of active cards in membership accounts",
                "FAIL",
                "Business Rule",
                field="cardCount; cardStatusCode; Membership_Number",
                expected="Required columns exist",
                actual="Missing columns",
            )
            return

        work = inf_df.copy()
        work["Membership_Number"] = self._trim_series(work["Membership_Number"])
        work["cardStatusCode"] = self._trim_series(work["cardStatusCode"])
        work["cardCount"] = self._trim_series(work["cardCount"])

        group_columns = ["Membership_Number"]
        uses_snapshot_grouping = False

        if BOTH_FILES_ARE_CONSOLIDATED and TIMESTAMP_COLUMN in work.columns:
            work[TIMESTAMP_COLUMN] = self._normalize_timestamp_series_for_grouping(work[TIMESTAMP_COLUMN])
            group_columns = ["Membership_Number", TIMESTAMP_COLUMN]
            uses_snapshot_grouping = True

        issues: List[Dict[str, object]] = []
        mismatch = 0

        for group_key, group in work.groupby(group_columns):
            membership = group_key[0] if isinstance(group_key, tuple) else group_key
            snapshot_value = group_key[1] if isinstance(group_key, tuple) and len(group_key) > 1 else ""

            active_cards = int(group["cardStatusCode"].isin(ACTIVE_CARD_STATUS_CODES).sum())

            declared_values = {
                v
                for v in group["cardCount"].dropna().astype(str).str.strip().tolist()
                if v != ""
            }

            if not declared_values:
                continue

            if len(declared_values) > 1:
                mismatch += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    record = {
                        "Membership_Number": membership,
                        "declared_cardCount_values": ", ".join(sorted(declared_values)),
                        "derived_active_cards": active_cards,
                    }
                    if uses_snapshot_grouping:
                        record[TIMESTAMP_COLUMN] = snapshot_value
                    issues.append(record)
                continue

            declared = next(iter(declared_values))

            try:
                declared_int = int(Decimal(declared))
            except Exception:
                mismatch += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    record = {
                        "Membership_Number": membership,
                        "declared_cardCount_values": declared,
                        "derived_active_cards": active_cards,
                    }
                    if uses_snapshot_grouping:
                        record[TIMESTAMP_COLUMN] = snapshot_value
                    issues.append(record)
                continue

            if declared_int != active_cards:
                mismatch += 1
                if len(issues) < MAX_EVIDENCE_ROWS:
                    record = {
                        "Membership_Number": membership,
                        "declared_cardCount_values": declared,
                        "derived_active_cards": active_cards,
                    }
                    if uses_snapshot_grouping:
                        record[TIMESTAMP_COLUMN] = snapshot_value
                    issues.append(record)

        if issues:
            self.evidence["memberships_delta_cardcount_vs_active_cards"] = pd.DataFrame(issues)

        details = (
            "For consolidated files, this validation is executed by Membership_Number + Timestamp_Run to avoid false mismatches across different runs inside the same consolidated window."
            if uses_snapshot_grouping
            else "Validation is executed by Membership_Number."
        )

        self.add_check(
            "MEM-DELTA-021",
            "Validate cardCount correctly displays the number of active cards in membership accounts",
            "PASS" if mismatch == 0 else "FAIL",
            "Business Rule",
            field="cardCount; cardStatusCode; Membership_Number",
            expected="cardCount must equal number of active cards per validation group",
            actual=f"Mismatching groups={mismatch}",
            details=details,
            evidence_file="memberships_delta_cardcount_vs_active_cards.csv" if mismatch > 0 else "",
        )

    def _validate_timestamp(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        if TIMESTAMP_COLUMN not in domo_df.columns or TIMESTAMP_COLUMN not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-009",
                "Validate Domo and Informatica timestamp consistency",
                "FAIL",
                "Quality",
                field=TIMESTAMP_COLUMN,
                expected="Timestamp column exists in both files",
                actual="Missing in one or both files",
            )
            return

        domo_ts = pd.to_datetime(
            self._trim_series(domo_df[TIMESTAMP_COLUMN]).replace("", pd.NA),
            errors="coerce",
        ).dropna()
        inf_ts = pd.to_datetime(
            self._trim_series(inf_df[TIMESTAMP_COLUMN]).replace("", pd.NA),
            errors="coerce",
        ).dropna()

        if domo_ts.empty or inf_ts.empty:
            self.add_check(
                "MEM-DELTA-009",
                "Validate Domo and Informatica timestamp consistency",
                "FAIL",
                "Quality",
                field=TIMESTAMP_COLUMN,
                expected="Valid timestamps in both files",
                actual="One or both files do not contain valid timestamps",
            )
            return

        if TIMESTAMP_COMPARE_DATE_ONLY:
            domo_min = domo_ts.min().date().isoformat()
            domo_max = domo_ts.max().date().isoformat()
            inf_min = inf_ts.min().date().isoformat()
            inf_max = inf_ts.max().date().isoformat()

            same_effective_date = domo_max == inf_max

            self.add_check(
                "MEM-DELTA-009",
                "Validate Domo and Informatica timestamp consistency",
                "PASS" if same_effective_date else "FAIL",
                "Quality",
                field=TIMESTAMP_COLUMN,
                expected=f"{domo_min} -> {domo_max}",
                actual=f"{inf_min} -> {inf_max}",
                details="Timestamp validation is configured to compare only the date portion and ignore the time component.",
            )
            return

        domo_spread = (domo_ts.max() - domo_ts.min()).total_seconds() / 60.0
        inf_spread = (inf_ts.max() - inf_ts.min()).total_seconds() / 60.0
        expected_window = self._effective_window_minutes()

        same_range = domo_ts.min() == inf_ts.min() and domo_ts.max() == inf_ts.max()

        if same_range:
            status = "PASS"
            details = ""
        elif BOTH_FILES_ARE_CONSOLIDATED:
            status = (
                "PASS"
                if domo_spread <= expected_window and inf_spread <= expected_window
                else "FAIL"
            )
            details = (
                f"Both files are consolidated. Each file must stay within a maximum spread of {expected_window} minutes."
            )
        elif DOMO_IS_CONSOLIDATED:
            status = "WARNING" if DELTA_ALLOW_TIMESTAMP_RANGE_DIFFERENCE else "FAIL"
            details = (
                f"Domo is treated as a consolidated file spanning multiple 5-minute generations across up to {DOMO_CONSOLIDATED_WINDOW_MINUTES} minutes."
            )
        else:
            status = "FAIL" if self._same_snapshot() else ("WARNING" if DELTA_ALLOW_TIMESTAMP_RANGE_DIFFERENCE else "FAIL")
            details = (
                "Timestamp range differs and snapshots are the same, so this is treated as a real inconsistency."
                if self._same_snapshot()
                else "Timestamp range differs, but snapshots are different or undocumented; in Delta mode this is informational."
            )

        self.add_check(
            "MEM-DELTA-009",
            "Validate Domo and Informatica timestamp consistency",
            status,
            "Quality",
            field=TIMESTAMP_COLUMN,
            expected=f"{domo_ts.min()} -> {domo_ts.max()}",
            actual=f"{inf_ts.min()} -> {inf_ts.max()}",
            details=details,
        )

    def _validate_delta_window(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        domo_snapshot_raw = self.snapshot_context.domo_snapshot.strip()
        inf_snapshot_raw = self.snapshot_context.informatica_snapshot.strip()

        domo_snapshot = self._parse_snapshot_datetime(domo_snapshot_raw)
        inf_snapshot = self._parse_snapshot_datetime(inf_snapshot_raw)

        if domo_snapshot is None or inf_snapshot is None:
            self.add_check(
                "MEM-DELTA-009A",
                "Verify Delta comparison window is documented",
                "WARNING",
                "File Control",
                field="Snapshot / delta window",
                expected="Snapshot datetime values are provided for both files",
                actual="Missing or non-parseable snapshot datetime values",
                details="Delta window validation requires snapshot datetime inputs.",
            )
            return

        if TIMESTAMP_COLUMN not in domo_df.columns or TIMESTAMP_COLUMN not in inf_df.columns:
            self.add_check(
                "MEM-DELTA-009A",
                "Verify Delta comparison window is documented",
                "FAIL",
                "File Control",
                field=TIMESTAMP_COLUMN,
                expected="Timestamp column exists in both files",
                actual="Missing in one or both files",
            )
            return

        if TIMESTAMP_COMPARE_DATE_ONLY:
            domo_ts = self._normalize_timestamp_series_for_date_only(domo_df[TIMESTAMP_COLUMN])
            inf_ts = self._normalize_timestamp_series_for_date_only(inf_df[TIMESTAMP_COLUMN])

            domo_snapshot_date = pd.Timestamp(domo_snapshot.date())
            inf_snapshot_date = pd.Timestamp(inf_snapshot.date())

            domo_mask = domo_ts.eq(domo_snapshot_date).fillna(False)
            inf_mask = inf_ts.eq(inf_snapshot_date).fillna(False)

            domo_outside = domo_df.loc[~domo_mask].copy()
            inf_outside = inf_df.loc[~inf_mask].copy()

            if not domo_outside.empty:
                self.evidence["memberships_delta_domo_outside_window"] = domo_outside.head(MAX_EVIDENCE_ROWS)
            if not inf_outside.empty:
                self.evidence["memberships_delta_infa_outside_window"] = inf_outside.head(MAX_EVIDENCE_ROWS)

            self.add_check(
                "MEM-DELTA-009B",
                "Verify Delta contains only memberships changed within the configured production window",
                "PASS" if domo_outside.empty and inf_outside.empty else "FAIL",
                "Delta Logic",
                field=TIMESTAMP_COLUMN,
                expected=(
                    f"All rows must match snapshot date only. "
                    f"Domo date: {domo_snapshot_date.date().isoformat()}; "
                    f"Informatica date: {inf_snapshot_date.date().isoformat()}"
                ),
                actual=(
                    f"Domo rows outside date={len(domo_outside)} | "
                    f"Informatica rows outside date={len(inf_outside)}"
                ),
                details="Timestamp_Run validation is configured to compare only the date portion and ignore the time component.",
                evidence_file=(
                    "memberships_delta_domo_outside_window.csv"
                    if not domo_outside.empty
                    else ("memberships_delta_infa_outside_window.csv" if not inf_outside.empty else "")
                ),
            )

            if BOTH_FILES_ARE_CONSOLIDATED:
                status = "PASS" if domo_snapshot_date == inf_snapshot_date else "FAIL"
                details = "Both files are consolidated and Timestamp_Run is configured to validate only the snapshot date."
            elif DOMO_IS_CONSOLIDATED:
                status = "WARNING"
                details = (
                    "Domo is treated as a consolidated file, so exact snapshot date equality remains informational "
                    "when comparing different documented runs in Delta mode."
                )
            else:
                status = "PASS" if domo_snapshot_date == inf_snapshot_date else "FAIL"
                details = "Timestamp_Run is configured to validate only the date portion of the documented snapshot."

            self.add_check(
                "MEM-DELTA-009C",
                "Verify Domo and Informatica use the same effective Delta window",
                status,
                "File Control",
                field="Snapshot / delta window",
                expected=domo_snapshot_date.date().isoformat(),
                actual=inf_snapshot_date.date().isoformat(),
                details=details,
            )
            return

        domo_ts = pd.to_datetime(
            self._trim_series(domo_df[TIMESTAMP_COLUMN]).replace("", pd.NA),
            errors="coerce",
        )
        inf_ts = pd.to_datetime(
            self._trim_series(inf_df[TIMESTAMP_COLUMN]).replace("", pd.NA),
            errors="coerce",
        )

        effective_window_minutes = self._effective_window_minutes()

        domo_window_start = domo_snapshot - pd.Timedelta(minutes=effective_window_minutes)
        inf_window_start = inf_snapshot - pd.Timedelta(minutes=effective_window_minutes)

        domo_mask = domo_ts.between(domo_window_start, domo_snapshot, inclusive="both").fillna(False)
        inf_mask = inf_ts.between(inf_window_start, inf_snapshot, inclusive="both").fillna(False)

        domo_outside = domo_df.loc[~domo_mask].copy()
        inf_outside = inf_df.loc[~inf_mask].copy()

        if not domo_outside.empty:
            self.evidence["memberships_delta_domo_outside_window"] = domo_outside.head(MAX_EVIDENCE_ROWS)
        if not inf_outside.empty:
            self.evidence["memberships_delta_infa_outside_window"] = inf_outside.head(MAX_EVIDENCE_ROWS)

        self.add_check(
            "MEM-DELTA-009B",
            "Verify Delta contains only memberships changed within the configured production window",
            "PASS" if domo_outside.empty and inf_outside.empty else "FAIL",
            "Delta Logic",
            field=TIMESTAMP_COLUMN,
            expected=(
                f"All rows must be within the last {effective_window_minutes} minutes. "
                f"Domo window: {domo_window_start} -> {domo_snapshot}; "
                f"Informatica window: {inf_window_start} -> {inf_snapshot}"
            ),
            actual=f"Outside window -> Domo={len(domo_outside)} | Informatica={len(inf_outside)}",
            details="Each consolidated file must stay inside its documented comparison window.",
            evidence_file=(
                "memberships_delta_infa_outside_window.csv"
                if not inf_outside.empty
                else ("memberships_delta_domo_outside_window.csv" if not domo_outside.empty else "")
            ),
        )

        if BOTH_FILES_ARE_CONSOLIDATED:
            status = "PASS" if self._snapshots_within_tolerance() else "FAIL"
            details = (
                f"Both files are consolidated. Snapshot values may differ up to {CONSOLIDATED_SNAPSHOT_TOLERANCE_MINUTES} minutes."
            )
        elif DOMO_IS_CONSOLIDATED:
            status = "WARNING"
            details = (
                f"Domo is treated as a consolidated file spanning multiple 5-minute generations across up to {DOMO_CONSOLIDATED_WINDOW_MINUTES} minutes, so exact effective-window equality is informational."
            )
        else:
            status = "PASS" if domo_window_start == inf_window_start and domo_snapshot == inf_snapshot else "FAIL"
            details = f"Configured Delta window size = {DELTA_WINDOW_MINUTES} minutes."

        self.add_check(
            "MEM-DELTA-009C",
            "Verify Domo and Informatica use the same effective Delta window",
            status,
            "File Control",
            field="Snapshot / delta window",
            expected=f"{domo_window_start} -> {domo_snapshot}",
            actual=f"{inf_window_start} -> {inf_snapshot}",
            details=details,
        )

    def _validate_cadence_support(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        expected_window = self._effective_window_minutes()

        for source_name, df, check_id in [
            ("Domo", domo_df, "MEM-DELTA-009D-DOMO"),
            ("Informatica", inf_df, "MEM-DELTA-009D-INFA"),
        ]:
            if TIMESTAMP_COLUMN not in df.columns:
                self.add_check(
                    check_id,
                    f"Verify that the {source_name} file can support the approved production cadence",
                    "FAIL",
                    "Delta Logic",
                    field=TIMESTAMP_COLUMN,
                    expected="Timestamp column exists",
                    actual="Column missing",
                )
                continue

            ts = pd.to_datetime(
                self._trim_series(df[TIMESTAMP_COLUMN]).replace("", pd.NA),
                errors="coerce",
            ).dropna()

            if ts.empty:
                self.add_check(
                    check_id,
                    f"Verify that the {source_name} file can support the approved production cadence",
                    "FAIL",
                    "Delta Logic",
                    field=TIMESTAMP_COLUMN,
                    expected="Valid timestamp values exist",
                    actual="No valid timestamps",
                )
                continue

            if TIMESTAMP_COMPARE_DATE_ONLY:
                spread_days = (ts.max().date() - ts.min().date()).days
                self.add_check(
                    check_id,
                    f"Verify that the {source_name} file can support the approved production cadence",
                    "PASS" if spread_days <= 1 else "FAIL",
                    "Delta Logic",
                    field=TIMESTAMP_COLUMN,
                    expected="Timestamp date spread <= 1 day",
                    actual=f"Observed date spread={spread_days} day(s)",
                    details="Cadence validation is configured to evaluate only the date portion of Timestamp_Run and ignore the time component.",
                )
                continue

            spread_minutes = (ts.max() - ts.min()).total_seconds() / 60.0
            self.add_check(
                check_id,
                f"Verify that the {source_name} file can support the approved production cadence",
                "PASS" if spread_minutes <= expected_window else "FAIL",
                "Delta Logic",
                field=TIMESTAMP_COLUMN,
                expected=f"Timestamp spread <= {expected_window} minutes",
                actual=f"Observed spread={spread_minutes:.2f} minutes",
            )

    def _validate_common_key_rows(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        missing_domo = [c for c in EXPECTED_COLUMNS_BASELINE if c not in domo_df.columns]
        missing_inf = [c for c in EXPECTED_COLUMNS_BASELINE if c not in inf_df.columns]

        if missing_domo or missing_inf:
            self.add_check(
                "MEM-DELTA-010",
                "Validate all 44 fields match",
                "FAIL",
                "Overall Reconciliation",
                field="All 44 delta fields",
                expected="All baseline columns exist",
                actual=f"Missing in Domo={len(missing_domo)} | Missing in Informatica={len(missing_inf)}",
            )
            return

        if not self._is_comparable_pair(domo_df, inf_df):
            status = "WARNING" if ALLOW_WARNING_WHEN_NOT_COMPARABLE else "FAIL"
            self.add_check(
                "MEM-DELTA-010",
                "Validate all 44 fields match",
                status,
                "Overall Reconciliation",
                field="All 44 delta fields",
                expected="Comparable Domo and Informatica files with overlapping primary keys from the same effective window",
                actual="Files are not comparable for strict cross-file field comparison",
                details="Comparable overlap is required for strict field-by-field validation.",
            )
            return

        common_keys_count, mismatch_count = self._compare_rows_on_common_primary_keys(domo_df, inf_df)

        self.add_check(
            "MEM-DELTA-010",
            "Validate all 44 fields match",
            "PASS" if mismatch_count == 0 else "FAIL",
            "Overall Reconciliation",
            field="All 44 delta fields",
            expected="Rows sharing the same primary keys must match in all 44 columns",
            actual=f"common_keys={common_keys_count} | mismatching_common_rows={mismatch_count}",
            details="For Delta mode, field-by-field validation is executed over overlapping memberships.",
            evidence_file="memberships_delta_common_key_row_diffs.csv" if mismatch_count > 0 else "",
        )

    def _validate_transition_presence_and_values(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        needed = {"Membership_Number", "Account_Number_Full", "membershipTypeCode", "isPlatinumMember", "effectiveDate", "Timestamp_Run"}
        if not needed.issubset(set(domo_df.columns)) or not needed.issubset(set(inf_df.columns)):
            self.add_check(
                "MEM-DELTA-022",
                "Validate Platinum to Diamond downgrade records are included correctly in the delta file",
                "FAIL",
                "Upgrade / Downgrade Validation",
                field="membershipTypeCode; isPlatinumMember; Membership_Number; effectiveDate; Timestamp_Run",
                expected="Required columns exist in both files",
                actual="Missing columns",
            )
            self.add_check(
                "MEM-DELTA-023",
                "Validate Diamond to Platinum upgrade records are included correctly in the delta file",
                "FAIL",
                "Upgrade / Downgrade Validation",
                field="membershipTypeCode; isPlatinumMember; platinumUpgradeProratedPrice; platinumamountsavedpromoamount; Membership_Number; effectiveDate; Timestamp_Run",
                expected="Required columns exist in both files",
                actual="Missing columns",
            )
            self.add_check(
                "MEM-DELTA-024",
                "Validate upgrade and downgrade transitions preserve exact Domo values in the delta file",
                "FAIL",
                "Upgrade / Downgrade Validation",
                field="; ".join(TRANSITION_VALUE_COLUMNS),
                expected="Required columns exist in both files",
                actual="Missing columns",
            )
            return

        if not self._is_comparable_pair(domo_df, inf_df):
            status = "WARNING" if ALLOW_WARNING_WHEN_NOT_COMPARABLE else "FAIL"
            details = "Comparable overlap is required for strict transition validation."

            self.add_check(
                "MEM-DELTA-022",
                "Validate Platinum to Diamond downgrade records are included correctly in the delta file",
                status,
                "Upgrade / Downgrade Validation",
                field="membershipTypeCode; isPlatinumMember; Membership_Number; effectiveDate; Timestamp_Run",
                expected="Comparable files with overlapping memberships",
                actual="Files are not comparable for strict downgrade cross-file validation",
                details=details,
            )
            self.add_check(
                "MEM-DELTA-023",
                "Validate Diamond to Platinum upgrade records are included correctly in the delta file",
                status,
                "Upgrade / Downgrade Validation",
                field="membershipTypeCode; isPlatinumMember; platinumUpgradeProratedPrice; platinumamountsavedpromoamount; Membership_Number; effectiveDate; Timestamp_Run",
                expected="Comparable files with overlapping memberships",
                actual="Files are not comparable for strict upgrade cross-file validation",
                details=details,
            )
            self.add_check(
                "MEM-DELTA-024",
                "Validate upgrade and downgrade transitions preserve exact Domo values in the delta file",
                status,
                "Upgrade / Downgrade Validation",
                field="; ".join(TRANSITION_VALUE_COLUMNS),
                expected="Comparable files with overlapping memberships",
                actual="Files are not comparable for strict transition value comparison",
                details=details,
            )
            return

        def is_downgrade_candidate(row: Dict[str, object]) -> bool:
            return (
                self._trim_value(row.get("membershipTypeCode", "")) in DOWNGRADE_MEMBERSHIP_TYPE_CODES
                and self._trim_value(row.get("isPlatinumMember", "")) == "0"
            )

        def is_upgrade_candidate(row: Dict[str, object]) -> bool:
            return (
                self._trim_value(row.get("membershipTypeCode", "")) in UPGRADE_MEMBERSHIP_TYPE_CODES
                and self._trim_value(row.get("isPlatinumMember", "")) == "1"
                and any(self._is_nonzero_numeric(row.get(col, "")) for col in UPGRADE_PRICE_COLUMNS)
            )

        def collect_transition_rows(df: pd.DataFrame, predicate) -> Dict[Tuple[str, ...], Dict[str, object]]:
            collected: Dict[Tuple[str, ...], Dict[str, object]] = {}
            for row in df.to_dict(orient="records"):
                if not predicate(row):
                    continue
                signature = self._build_transition_signature(row)
                if signature not in collected:
                    collected[signature] = row
            return collected

        domo_downgrade_rows = collect_transition_rows(domo_df, is_downgrade_candidate)
        inf_downgrade_rows = collect_transition_rows(inf_df, is_downgrade_candidate)

        domo_upgrade_rows = collect_transition_rows(domo_df, is_upgrade_candidate)
        inf_upgrade_rows = collect_transition_rows(inf_df, is_upgrade_candidate)

        domo_downgrade_keys = set(domo_downgrade_rows.keys())
        inf_downgrade_keys = set(inf_downgrade_rows.keys())

        domo_upgrade_keys = set(domo_upgrade_rows.keys())
        inf_upgrade_keys = set(inf_upgrade_rows.keys())

        downgrade_missing = sorted(domo_downgrade_keys - inf_downgrade_keys)
        upgrade_missing = sorted(domo_upgrade_keys - inf_upgrade_keys)

        if downgrade_missing:
            self.evidence["memberships_delta_downgrade_missing"] = pd.DataFrame(
                [
                    {
                        "Membership_Number": key[0],
                        "Account_Number_Full": key[1],
                        "effectiveDate": key[2],
                        "Timestamp_Run": key[3],
                        "membershipTypeCode": key[4],
                        "isPlatinumMember": key[5],
                    }
                    for key in downgrade_missing[:MAX_EVIDENCE_ROWS]
                ]
            )

        if upgrade_missing:
            self.evidence["memberships_delta_upgrade_missing"] = pd.DataFrame(
                [
                    {
                        "Membership_Number": key[0],
                        "Account_Number_Full": key[1],
                        "effectiveDate": key[2],
                        "Timestamp_Run": key[3],
                        "membershipTypeCode": key[4],
                        "isPlatinumMember": key[5],
                    }
                    for key in upgrade_missing[:MAX_EVIDENCE_ROWS]
                ]
            )

        self.add_check(
            "MEM-DELTA-022",
            "Validate Platinum to Diamond downgrade records are included correctly in the delta file",
            "PASS" if not downgrade_missing else "FAIL",
            "Upgrade / Downgrade Validation",
            field="membershipTypeCode; isPlatinumMember; Membership_Number; effectiveDate; Timestamp_Run",
            expected="All downgrade candidate rows present in Domo must exist in Informatica",
            actual=f"Missing downgrade rows={len(downgrade_missing)}",
            evidence_file="memberships_delta_downgrade_missing.csv" if downgrade_missing else "",
        )

        self.add_check(
            "MEM-DELTA-023",
            "Validate Diamond to Platinum upgrade records are included correctly in the delta file",
            "PASS" if not upgrade_missing else "FAIL",
            "Upgrade / Downgrade Validation",
            field="membershipTypeCode; isPlatinumMember; platinumUpgradeProratedPrice; platinumamountsavedpromoamount; Membership_Number; effectiveDate; Timestamp_Run",
            expected="All upgrade candidate rows present in Domo must exist in Informatica",
            actual=f"Missing upgrade rows={len(upgrade_missing)}",
            evidence_file="memberships_delta_upgrade_missing.csv" if upgrade_missing else "",
        )

        transition_common = sorted((domo_downgrade_keys | domo_upgrade_keys) & (set(inf_downgrade_rows.keys()) | set(inf_upgrade_rows.keys())))
        transition_mismatch = 0
        transition_records: List[Dict[str, object]] = []

        for key in transition_common:
            domo_row = domo_downgrade_rows.get(key) or domo_upgrade_rows.get(key)
            inf_row = inf_downgrade_rows.get(key) or inf_upgrade_rows.get(key)
            if domo_row is None or inf_row is None:
                continue

            diffs = []
            for col in TRANSITION_VALUE_COLUMNS:
                domo_val = self._normalize_value_for_compare(col, domo_row.get(col, ""), "DOMO")
                inf_val = self._normalize_value_for_compare(col, inf_row.get(col, ""), "INFA")
                if domo_val != inf_val:
                    diffs.append(col)
            if diffs:
                transition_mismatch += 1
                if len(transition_records) < MAX_EVIDENCE_ROWS:
                    transition_records.append(
                        {
                            "Membership_Number": key[0],
                            "Account_Number_Full": key[1],
                            "effectiveDate": key[2],
                            "Timestamp_Run": key[3],
                            "different_fields": ", ".join(diffs),
                        }
                    )

        if transition_records:
            self.evidence["memberships_delta_transition_value_diffs"] = pd.DataFrame(transition_records)

        self.add_check(
            "MEM-DELTA-024",
            "Validate upgrade and downgrade transitions preserve exact Domo values in the delta file",
            "PASS" if transition_mismatch == 0 else "FAIL",
            "Upgrade / Downgrade Validation",
            field="; ".join(TRANSITION_VALUE_COLUMNS),
            expected="Transition rows common to both files must preserve exact Domo values",
            actual=f"Common transition rows={len(transition_common)} | mismatches={transition_mismatch}",
            evidence_file="memberships_delta_transition_value_diffs.csv" if transition_mismatch > 0 else "",
        )

    def _validate_full_rows(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> None:
        missing_domo = [c for c in EXPECTED_COLUMNS_BASELINE if c not in domo_df.columns]
        missing_inf = [c for c in EXPECTED_COLUMNS_BASELINE if c not in inf_df.columns]

        if missing_domo or missing_inf:
            self.add_check(
                "MEM-DELTA-010B",
                "Verify full Delta row reconciliation",
                "FAIL",
                "Overall Reconciliation",
                field="All baseline columns",
                expected="All baseline columns exist",
                actual=f"Missing in Domo={len(missing_domo)} | Missing in Informatica={len(missing_inf)}",
            )
            return

        domo_projected = self._project_baseline(domo_df)
        inf_projected = self._project_baseline(inf_df)

        domo_counts = Counter(self._normalized_rows(domo_projected))
        inf_counts = Counter(self._normalized_rows(inf_projected))

        diffs: List[Tuple[str, ...]] = []
        for row in set(domo_counts) | set(inf_counts):
            if domo_counts.get(row, 0) != inf_counts.get(row, 0):
                diffs.append(row)

        if diffs:
            self.evidence["memberships_delta_full_row_diffs"] = pd.DataFrame(
                [dict(zip(EXPECTED_COLUMNS_BASELINE, row)) for row in diffs[:MAX_EVIDENCE_ROWS]]
            )

        self.add_check(
            "MEM-DELTA-010B",
            "Verify full Delta row reconciliation",
            "PASS" if not diffs else "FAIL",
            "Overall Reconciliation",
            field="All baseline columns",
            expected="Exact normalized row multiset equivalence on raw baseline columns",
            actual=f"Different rows={len(diffs)}",
            details="Domo technical enrichment columns are excluded before row-level comparison.",
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
            details="Warnings do not block the overall decision in Delta mode.",
        )

    def _build_summary(self, domo_df: pd.DataFrame, inf_df: pd.DataFrame) -> Dict[str, object]:
        status_counts = Counter(c.status for c in self.checks)

        domo_country_population = sorted(
            {
                self._normalize_country_alpha2(value)
                for col in COUNTRY_COLUMNS
                if col in domo_df.columns
                for value in self._trim_series(domo_df[col]).tolist()
                if self._normalize_country_alpha2(value)
            }
        )
        inf_country_population = sorted(
            {
                self._normalize_country_alpha2(value)
                for col in COUNTRY_COLUMNS
                if col in inf_df.columns
                for value in self._trim_series(inf_df[col]).tolist()
                if self._normalize_country_alpha2(value)
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