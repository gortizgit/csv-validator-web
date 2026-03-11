from __future__ import annotations

import os
from datetime import datetime
from typing import Dict

import pandas as pd

from prices_validator.core.types import ValidationRun


def ensure_out_dir(out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def _build_issue_explanations_df(checks_df: pd.DataFrame) -> pd.DataFrame:
    if checks_df.empty:
        return pd.DataFrame(
            columns=[
                "check_id",
                "status",
                "check_name",
                "country",
                "field",
                "actual",
                "details",
                "evidence_file",
            ]
        )

    issues_df = checks_df[checks_df["status"].isin(["FAIL", "WARNING"])].copy()

    if issues_df.empty:
        return pd.DataFrame(
            columns=[
                "check_id",
                "status",
                "check_name",
                "country",
                "field",
                "actual",
                "details",
                "evidence_file",
            ]
        )

    wanted_columns = [
        "check_id",
        "status",
        "check_name",
        "country",
        "field",
        "actual",
        "details",
        "evidence_file",
    ]

    for col in wanted_columns:
        if col not in issues_df.columns:
            issues_df[col] = ""

    return issues_df[wanted_columns]


def write_reports(run: ValidationRun, out_dir: str) -> Dict[str, str]:
    ensure_out_dir(out_dir)

    checks_df = pd.DataFrame([c.to_dict() for c in run.checks])
    checks_csv = os.path.join(out_dir, "check_results.csv")
    checks_df.to_csv(checks_csv, index=False)

    issues_df = _build_issue_explanations_df(checks_df)
    issue_explanations_csv = os.path.join(out_dir, "issue_explanations.csv")
    issues_df.to_csv(issue_explanations_csv, index=False)

    summary_md = os.path.join(out_dir, "summary.md")
    with open(summary_md, "w", encoding="utf-8") as fh:
        fh.write("# Prices Validation Report\n\n")
        fh.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n\n")

        for key, value in run.summary.items():
            fh.write(f"- **{key}**: {value}\n")

        if not issues_df.empty:
            fh.write("\n## Warnings and Failures Explained\n\n")
            for _, row in issues_df.iterrows():
                fh.write(f"### {row['check_id']} — {row['status']}\n\n")
                fh.write(f"**Check:** {row['check_name']}\n\n")

                if str(row.get("country", "")).strip():
                    fh.write(f"**Country:** {row['country']}\n\n")

                if str(row.get("field", "")).strip():
                    fh.write(f"**Field:** {row['field']}\n\n")

                if str(row.get("actual", "")).strip():
                    fh.write(f"**Observed:** {row['actual']}\n\n")

                if str(row.get("details", "")).strip():
                    fh.write(f"**Explanation:** {row['details']}\n\n")
                else:
                    fh.write("**Explanation:** No additional explanation was provided.\n\n")

                if str(row.get("evidence_file", "")).strip():
                    fh.write(f"**Evidence:** {row['evidence_file']}\n\n")

    evidence_paths: Dict[str, str] = {
        "summary": summary_md,
        "checks": checks_csv,
        "issue_explanations": issue_explanations_csv,
    }

    dataframes = run.dataframes or {}
    for name, df in dataframes.items():
        if df is None:
            continue
        path = os.path.join(out_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        evidence_paths[name] = path

    excel_path = os.path.join(out_dir, "validation_report.xlsx")
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        pd.DataFrame([run.summary]).to_excel(writer, sheet_name="Summary", index=False)
        checks_df.to_excel(writer, sheet_name="Checks", index=False)
        issues_df.to_excel(writer, sheet_name="Issue_Explanations", index=False)

        for name, df in dataframes.items():
            if df is None:
                continue
            safe_name = name[:31] if len(name) > 31 else name
            df.to_excel(writer, sheet_name=safe_name, index=False)

    evidence_paths["excel"] = excel_path

    return evidence_paths