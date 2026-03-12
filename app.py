from __future__ import annotations

import io
import sys
import zipfile
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from prices_validator.core.csv_loader import load_csv_raw
from prices_validator.core.report_writer import write_reports
from prices_validator.core.types import SnapshotContext
from prices_validator.validators.prices_validator import PricesValidator

APP_TITLE = "CSV Validation Workbench"
REPORTS_DIR = ROOT / "reports"
TMP_UPLOAD_DIR = ROOT / ".streamlit_uploads"

DATASETS = ["Prices", "Products", "UPS", "Currencies", "Memberships"]


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_uploaded_file(uploaded_file, destination: Path) -> Path:
    ensure_dir(destination.parent)
    with open(destination, "wb") as fh:
        fh.write(uploaded_file.getbuffer())
    return destination


def zip_directory(folder_path: Path, zip_path: Path) -> Path:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path in folder_path.rglob("*"):
            if file_path.is_file() and file_path != zip_path:
                zf.write(file_path, arcname=file_path.relative_to(folder_path))
    return zip_path


def read_text_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def build_run_name(dataset_name: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{dataset_name.lower()}_run_{ts}"


def dataset_supported(dataset_name: str) -> bool:
    return dataset_name == "Prices"


def render_dataset_help(dataset_name: str) -> None:
    if dataset_name == "Prices":
        st.info(
            "Prices validator is active. It supports schema, record count, key population, "
            "blank/whitespace behavior, country-level validation, cross-field alignment, "
            "Excel report, issue explanations, and ZIP export."
        )
    else:
        st.warning(
            f"{dataset_name} is not implemented yet. "
            "The UI is already prepared so we can plug the validator in next."
        )


def run_prices_validation(
    domo_path: Path,
    inf_path: Path,
    delimiter: str,
    encoding: str,
    run_dir: Path,
    domo_snapshot: str,
    inf_snapshot: str,
) -> Dict[str, str]:
    print("Loading DOMO CSV...")
    domo_df = load_csv_raw(str(domo_path), delimiter=delimiter, encoding=encoding)
    print(f"DOMO loaded: rows={len(domo_df):,}, columns={len(domo_df.columns):,}")

    print("Loading INFORMATICA CSV...")
    inf_df = load_csv_raw(str(inf_path), delimiter=delimiter, encoding=encoding)
    print(f"INFORMATICA loaded: rows={len(inf_df):,}, columns={len(inf_df.columns):,}")

    print("Initializing Prices validator...")
    validator = PricesValidator(
        snapshot_context=SnapshotContext(
            domo_snapshot=domo_snapshot.strip(),
            informatica_snapshot=inf_snapshot.strip(),
        )
    )

    print("Running Prices validation...")
    run = validator.validate(domo_df, inf_df)

    print("Writing reports...")
    evidence_paths = write_reports(run, str(run_dir))
    print("Reports generated successfully.")

    return evidence_paths


def show_file_downloads(evidence_paths: Dict[str, str], run_dir: Path) -> None:
    st.subheader("Downloads")

    summary_path = Path(evidence_paths.get("summary", ""))
    checks_path = Path(evidence_paths.get("checks", ""))
    excel_path = Path(evidence_paths.get("excel", ""))
    issues_path = Path(evidence_paths.get("issue_explanations", ""))

    col1, col2, col3, col4 = st.columns(4)

    if summary_path.exists():
        with col1:
            st.download_button(
                "Download summary.md",
                data=summary_path.read_bytes(),
                file_name=summary_path.name,
                mime="text/markdown",
                key=f"download_summary_{run_dir.name}",
            )

    if checks_path.exists():
        with col2:
            st.download_button(
                "Download check_results.csv",
                data=checks_path.read_bytes(),
                file_name=checks_path.name,
                mime="text/csv",
                key=f"download_checks_{run_dir.name}",
            )

    if excel_path.exists():
        with col3:
            st.download_button(
                "Download validation_report.xlsx",
                data=excel_path.read_bytes(),
                file_name=excel_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_excel_{run_dir.name}",
            )

    if issues_path.exists():
        with col4:
            st.download_button(
                "Download issue_explanations.csv",
                data=issues_path.read_bytes(),
                file_name=issues_path.name,
                mime="text/csv",
                key=f"download_issues_{run_dir.name}",
            )

    zip_path = run_dir / f"{run_dir.name}.zip"
    zip_directory(run_dir, zip_path)

    st.download_button(
        "Download full run as ZIP",
        data=zip_path.read_bytes(),
        file_name=zip_path.name,
        mime="application/zip",
        key=f"download_zip_{run_dir.name}",
    )


def render_run_outputs(evidence_paths: Dict[str, str]) -> None:
    summary_path = Path(evidence_paths.get("summary", ""))
    checks_path = Path(evidence_paths.get("checks", ""))
    issues_path = Path(evidence_paths.get("issue_explanations", ""))

    if summary_path.exists():
        st.subheader("Summary")
        st.markdown(read_text_file(summary_path))

    if checks_path.exists():
        st.subheader("Checks")
        checks_df = pd.read_csv(checks_path)
        st.dataframe(checks_df, use_container_width=True, height=420)

    if issues_path.exists():
        issues_df = pd.read_csv(issues_path)
        if not issues_df.empty:
            st.subheader("Warnings / Failures Explained")
            st.dataframe(issues_df, use_container_width=True, height=260)


def init_session_state() -> None:
    defaults = {
        "last_evidence_paths": None,
        "last_run_dir": None,
        "last_log_text": "",
        "last_status": None,
        "validation_counter": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def clear_validation_state() -> None:
    st.session_state["last_evidence_paths"] = None
    st.session_state["last_run_dir"] = None
    st.session_state["last_log_text"] = ""
    st.session_state["last_status"] = None
    st.session_state["validation_counter"] = st.session_state.get("validation_counter", 0) + 1


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    ensure_dir(REPORTS_DIR)
    ensure_dir(TMP_UPLOAD_DIR)
    init_session_state()

    top_left, top_right = st.columns([6, 1])
    with top_left:
        st.title(APP_TITLE)
        st.caption("Multi-dataset validation console for DOMO / Informatica style comparisons")
    with top_right:
        if st.button("New Validation", use_container_width=True):
            clear_validation_state()
            st.rerun()

    with st.sidebar:
        st.header("Validation Setup")

        selected_dataset = st.selectbox("Dataset", DATASETS, index=0)
        delimiter = st.text_input("Delimiter", value=",")
        encoding = st.text_input("Encoding", value="utf-8")
        run_name = st.text_input("Run name", value=build_run_name(selected_dataset))

        st.markdown("---")
        st.subheader("Optional snapshot values")
        domo_snapshot = st.text_input("DOMO snapshot / comparison window", value="")
        inf_snapshot = st.text_input("Informatica snapshot / comparison window", value="")

        st.markdown("---")
        st.subheader("About this run")
        render_dataset_help(selected_dataset)

    st.subheader(f"{selected_dataset} Validation")
    st.info("Large CSV files may take 1–5 minutes depending on file size and validation depth.")

    uploader_key_suffix = st.session_state["validation_counter"]

    left, right = st.columns(2)

    with left:
        domo_file = st.file_uploader(
            "Upload DOMO CSV",
            type=["csv"],
            key=f"{selected_dataset}_domo_{uploader_key_suffix}",
        )

    with right:
        inf_file = st.file_uploader(
            "Upload Informatica CSV",
            type=["csv"],
            key=f"{selected_dataset}_inf_{uploader_key_suffix}",
        )

    validate_clicked = st.button("Validate", type="primary")

    if validate_clicked:
        if not dataset_supported(selected_dataset):
            st.session_state["last_status"] = ("error", f"{selected_dataset} validator is not implemented yet.")
            st.rerun()

        if not domo_file or not inf_file:
            st.session_state["last_status"] = ("error", "Please upload both files.")
            st.rerun()

        safe_run_name = run_name.strip() or build_run_name(selected_dataset)
        run_dir = ensure_dir(REPORTS_DIR / safe_run_name)
        upload_dir = ensure_dir(TMP_UPLOAD_DIR / safe_run_name)

        domo_path = save_uploaded_file(domo_file, upload_dir / domo_file.name)
        inf_path = save_uploaded_file(inf_file, upload_dir / inf_file.name)

        log_buffer = io.StringIO()
        evidence_paths: Optional[Dict[str, str]] = None

        with st.spinner("Validating... large files may take time"):
            try:
                with redirect_stdout(log_buffer):
                    print("===================================")
                    print("VALIDATION WORKBENCH START")
                    print("===================================")
                    print(f"Dataset: {selected_dataset}")
                    print(f"DOMO file: {domo_path}")
                    print(f"Informatica file: {inf_path}")
                    print(f"Run folder: {run_dir}")
                    print("-----------------------------------")

                    evidence_paths = run_prices_validation(
                        domo_path=domo_path,
                        inf_path=inf_path,
                        delimiter=delimiter,
                        encoding=encoding,
                        run_dir=run_dir,
                        domo_snapshot=domo_snapshot,
                        inf_snapshot=inf_snapshot,
                    )

                    print("-----------------------------------")
                    print("Validation finished successfully")

                st.session_state["last_evidence_paths"] = evidence_paths
                st.session_state["last_run_dir"] = str(run_dir)
                st.session_state["last_log_text"] = log_buffer.getvalue()
                st.session_state["last_status"] = ("success", "Validation completed successfully.")

            except Exception as exc:
                print("-----------------------------------", file=log_buffer)
                print("Validation failed", file=log_buffer)
                print(type(exc).__name__, file=log_buffer)
                print(str(exc), file=log_buffer)

                st.session_state["last_evidence_paths"] = None
                st.session_state["last_run_dir"] = None
                st.session_state["last_log_text"] = log_buffer.getvalue()
                st.session_state["last_status"] = ("error", f"Validation failed: {exc}")

        st.rerun()

    if st.session_state["last_status"]:
        status_type, status_message = st.session_state["last_status"]
        st.subheader("Execution Status")
        if status_type == "success":
            st.success(status_message)
        elif status_type == "error":
            st.error(status_message)
        else:
            st.info(status_message)

    if st.session_state["last_log_text"]:
        st.subheader("Execution Log")
        st.code(st.session_state["last_log_text"], language="text")

    last_evidence_paths = st.session_state.get("last_evidence_paths")
    last_run_dir = st.session_state.get("last_run_dir")

    if last_evidence_paths and last_run_dir:
        render_run_outputs(last_evidence_paths)
        show_file_downloads(last_evidence_paths, Path(last_run_dir))

    st.markdown("---")
    st.subheader("Roadmap")
    st.markdown(
        """
        - Prices ✅  
        - Products ⏳  
        - UPS ⏳  
        - Currencies ⏳  
        - Memberships ⏳
        """
    )


if __name__ == "__main__":
    main()