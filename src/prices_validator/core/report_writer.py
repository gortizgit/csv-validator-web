from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List

import pandas as pd

from prices_validator.core.types import CheckResult, ValidationRun


COUNTRY_CODE_MAP = {
    "52": "BB",
    "052": "BB",
    "61": "TT",
    "62": "CR",
    "63": "TT",
    "64": "CR",
    "66": "BB",
    "67": "AW",
    "68": "PA",
    "80": "DO",
    "81": "GT",
    "82": "HN",
    "85": "SV",
    "87": "NI",
    "89": "JM",
    "188": "CR",
    "214": "DO",
    "222": "SV",
    "320": "GT",
    "340": "HN",
    "388": "JM",
    "533": "AW",
    "558": "NI",
    "591": "PA",
    "780": "TT",
}


def ensure_out_dir(out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def format_country_value(country_value: object) -> str:

    if country_value is None:
        return ""

    value = str(country_value).strip()

    if not value:
        return ""

    return COUNTRY_CODE_MAP.get(value, value)


# ✅ FIX AQUI — country se convierte antes de escribir reportes
def _checks_to_dataframe(checks: List[CheckResult]) -> pd.DataFrame:

    rows = []

    for c in checks:

        country_str = format_country_value(c.country)

        rows.append(
            {
                "check_id": c.check_id,
                "status": c.status,
                "check_name": c.check_name,
                "category": c.category,
                "dataset": c.dataset,
                "country": country_str,
                "field": c.field,
                "expected": c.expected,
                "actual": c.actual,
                "details": c.details,
                "evidence_file": c.evidence_file,
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "check_id",
            "status",
            "check_name",
            "category",
            "dataset",
            "country",
            "field",
            "expected",
            "actual",
            "details",
            "evidence_file",
        ],
    )


def _build_issue_explanations_df(checks_df: pd.DataFrame) -> pd.DataFrame:

    expected_columns = [
        "check_id",
        "status",
        "check_name",
        "category",
        "dataset",
        "country",
        "field",
        "expected",
        "actual",
        "details",
        "evidence_file",
        "explanation",
        "recommended_action",
        "priority",
    ]

    if checks_df.empty:
        return pd.DataFrame(columns=expected_columns)

    issues_df = checks_df[checks_df["status"].isin(["FAIL", "WARNING"])].copy()

    if issues_df.empty:
        return pd.DataFrame(columns=expected_columns)

    def build_explanation(row):

        status = str(row.get("status", "")).strip().upper()
        check_name = str(row.get("check_name", "")).strip()
        field = str(row.get("field", "")).strip()
        country = format_country_value(row.get("country", ""))
        expected = str(row.get("expected", "")).strip()
        actual = str(row.get("actual", "")).strip()
        details = str(row.get("details", "")).strip()

        parts = []

        if status == "FAIL":
            parts.append(f"This validation failed: {check_name}.")
        elif status == "WARNING":
            parts.append(f"This validation requires attention: {check_name}.")
        else:
            parts.append(f"Review required for: {check_name}.")

        if field and country:
            parts.append(f"Field or scope reviewed: {field} ({country}).")
        elif field:
            parts.append(f"Field or scope reviewed: {field}.")
        elif country:
            parts.append(f"Country: {country}.")

        if expected:
            parts.append(f"Expected result: {expected}.")
        if actual:
            parts.append(f"Observed result: {actual}.")
        if details:
            parts.append(f"Additional details: {details}.")

        return " ".join(parts)

    def build_recommended_action(row):

        status = str(row.get("status", "")).strip().upper()
        category = str(row.get("category", "")).strip().lower()
        field = str(row.get("field", "")).strip()
        country = format_country_value(row.get("country", ""))
        evidence_file = str(row.get("evidence_file", "")).strip()

        actions = []

        if category == "schema":
            actions.append("Review schema, headers, missing columns.")
        elif category == "file control":
            actions.append("Review snapshot / business date alignment.")
        elif category == "volume":
            actions.append("Review row count differences.")
        elif category == "population":
            actions.append("Review key population / duplicates.")
        elif category == "quality":
            actions.append("Review null / blank / whitespace issues.")
        elif category == "overall reconciliation":
            actions.append("Review blocking validation errors.")
        else:
            actions.append("Review validation rule.")

        if field:
            actions.append(f"Field: {field}")

        if country:
            actions.append(f"Country: {country}")

        if evidence_file:
            actions.append(f"See file: {evidence_file}")

        if status == "WARNING":
            actions.append("Check if warning is acceptable.")

        return ". ".join(actions)

    def build_priority(row):

        status = str(row.get("status", "")).upper()
        category = str(row.get("category", "")).lower()

        if status == "FAIL" and category in {
            "schema",
            "file control",
            "overall reconciliation",
        }:
            return "HIGH"

        if status == "FAIL":
            return "MEDIUM"

        if status == "WARNING":
            return "LOW"

        return "INFO"

    issues_df["explanation"] = issues_df.apply(build_explanation, axis=1)
    issues_df["recommended_action"] = issues_df.apply(build_recommended_action, axis=1)
    issues_df["priority"] = issues_df.apply(build_priority, axis=1)

    return issues_df[expected_columns].copy()


def _build_summary_markdown(
    run: ValidationRun,
    checks_df: pd.DataFrame,
    issues_df: pd.DataFrame,
) -> str:

    summary = run.summary or {}

    total_checks = len(checks_df.index)

    fail_count = int((checks_df["status"] == "FAIL").sum())
    warning_count = int((checks_df["status"] == "WARNING").sum())
    pass_count = int((checks_df["status"] == "PASS").sum())

    dataset = summary.get("dataset", "")
    overall_status = summary.get("overall_status", "UNKNOWN")

    lines = [
        "# Validation Summary",
        "",
        f"- Dataset: {dataset}",
        f"- Overall status: {overall_status}",
        "",
        f"- Total checks: {total_checks}",
        f"- Passed: {pass_count}",
        f"- Failed: {fail_count}",
        f"- Warnings: {warning_count}",
        "",
        f"- Explained rows: {len(issues_df)}",
        "",
        f"- Generated at: {datetime.now()}",
    ]

    return "\n".join(lines)


def _write_excel_report(
    excel_path: str,
    summary_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    evidence_frames: Dict[str, pd.DataFrame],
):

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:

        summary_df.to_excel(writer, sheet_name="summary", index=False)
        checks_df.to_excel(writer, sheet_name="checks", index=False)
        issues_df.to_excel(writer, sheet_name="issue_explanations", index=False)

        for name, df in evidence_frames.items():

            if df is None:
                df = pd.DataFrame()

            sheet = str(name)[:31]

            df.to_excel(writer, sheet_name=sheet, index=False)


def write_reports(run: ValidationRun, out_dir: str) -> Dict[str, str]:

    ensure_out_dir(out_dir)

    checks_df = _checks_to_dataframe(run.checks)

    issues_df = _build_issue_explanations_df(checks_df)

    summary_md = os.path.join(out_dir, "summary.md")
    checks_csv = os.path.join(out_dir, "check_results.csv")
    issues_csv = os.path.join(out_dir, "issue_explanations.csv")
    excel_path = os.path.join(out_dir, "validation_report.xlsx")

    checks_df.to_csv(checks_csv, index=False)
    issues_df.to_csv(issues_csv, index=False)

    summary_df = pd.DataFrame([run.summary or {}])

    summary_text = _build_summary_markdown(
        run,
        checks_df,
        issues_df,
    )

    with open(summary_md, "w", encoding="utf-8") as f:
        f.write(summary_text)

    _write_excel_report(
        excel_path,
        summary_df,
        checks_df,
        issues_df,
        run.dataframes or {},
    )

    evidence_paths = {
        "summary": summary_md,
        "checks": checks_csv,
        "issue_explanations": issues_csv,
        "excel": excel_path,
    }

    for name, df in (run.dataframes or {}).items():

        path = os.path.join(out_dir, f"{name}.csv")

        if df is None:
            df = pd.DataFrame()

        df.to_csv(path, index=False)

        evidence_paths[name] = path

    return evidence_paths