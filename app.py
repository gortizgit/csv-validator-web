from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime

import pandas as pd
import streamlit as st

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from prices_validator.core.csv_loader import load_csv_raw
from prices_validator.core.report_writer import write_reports
from prices_validator.core.types import SnapshotContext
from prices_validator.validators.prices_validator import PricesValidator

st.set_page_config(page_title="Prices Validator", layout="wide")
st.title("Prices Validator — Domo vs Informatica")
st.caption("Strict validation base project for the Prices dataset")

col1, col2, col3 = st.columns(3)
delimiter = col1.text_input("Delimiter", value=",")
encoding = col2.text_input("Encoding", value="utf-8")
run_name = col3.text_input("Run name", value=f"prices_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

snap1, snap2 = st.columns(2)
domo_snapshot = snap1.text_input("Domo business snapshot / approved window (optional)", value="")
inf_snapshot = snap2.text_input("Informatica business snapshot / approved window (optional)", value="")

domo_file = st.file_uploader("Upload Domo CSV", type=["csv"])
inf_file = st.file_uploader("Upload Informatica CSV", type=["csv"])

if st.button("Validate Prices", type="primary"):
    if not domo_file or not inf_file:
        st.error("Please upload both files.")
    else:
        with tempfile.TemporaryDirectory() as tmp_dir:
            domo_path = os.path.join(tmp_dir, domo_file.name)
            inf_path = os.path.join(tmp_dir, inf_file.name)
            with open(domo_path, "wb") as fh:
                fh.write(domo_file.getbuffer())
            with open(inf_path, "wb") as fh:
                fh.write(inf_file.getbuffer())

            domo_df = load_csv_raw(domo_path, delimiter=delimiter, encoding=encoding)
            inf_df = load_csv_raw(inf_path, delimiter=delimiter, encoding=encoding)

            validator = PricesValidator(
                snapshot_context=SnapshotContext(
                    domo_snapshot=domo_snapshot,
                    informatica_snapshot=inf_snapshot,
                )
            )
            run = validator.validate(domo_df, inf_df)

            out_dir = os.path.join(ROOT, "reports", run_name)
            evidence = write_reports(run, out_dir)

            summary_df = pd.DataFrame([run.summary])
            checks_df = pd.DataFrame([c.to_dict() for c in run.checks])

            st.subheader("Summary")
            st.dataframe(summary_df, use_container_width=True)

            st.subheader("Checks")
            st.dataframe(checks_df, use_container_width=True, height=700)

            st.success(f"Validation completed. Reports stored in: {out_dir}")

            with open(evidence["checks"], "rb") as fh:
                st.download_button("Download check_results.csv", fh.read(), file_name="check_results.csv")
            with open(evidence["excel"], "rb") as fh:
                st.download_button("Download validation_report.xlsx", fh.read(), file_name="validation_report.xlsx")
