from __future__ import annotations

import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from prices_validator.core.csv_loader import load_csv_raw
from prices_validator.core.report_writer import write_reports
from prices_validator.core.types import SnapshotContext
from prices_validator.validators.prices_validator import PricesValidator
from products_validator.validators.products_validator import ProductsValidator
from currencies_validator.validators.currencies_validator import CurrenciesValidator


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CSV validator for Prices / Products / Currencies"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["Prices", "Products", "Currencies"],
        help="Dataset to validate",
    )
    parser.add_argument("--domo", required=True, help="Path to Domo CSV")
    parser.add_argument("--informatica", required=True, help="Path to Informatica CSV")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter")
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument(
        "--domo-snapshot",
        default="",
        help="Optional Domo business snapshot / approved comparison window",
    )
    parser.add_argument(
        "--informatica-snapshot",
        default="",
        help="Optional Informatica business snapshot / approved comparison window",
    )
    args = parser.parse_args()

    start_time = time.time()

    print("===================================")
    print(f"{args.dataset.upper()} VALIDATOR START")
    print("===================================")
    print(f"DOMO file: {args.domo}")
    print(f"INFORMATICA file: {args.informatica}")
    print(f"Output folder: {args.out}")
    print("-----------------------------------")

    print("Loading DOMO CSV...")
    t0 = time.time()
    domo_df = load_csv_raw(args.domo, delimiter=args.delimiter, encoding=args.encoding)
    print("DOMO loaded successfully")
    print(f"DOMO rows: {len(domo_df):,}")
    print(f"DOMO columns: {len(domo_df.columns):,}")
    print(f"DOMO load time: {time.time() - t0:.2f}s")
    print("-----------------------------------")

    print("Loading INFORMATICA CSV...")
    t0 = time.time()
    inf_df = load_csv_raw(args.informatica, delimiter=args.delimiter, encoding=args.encoding)
    print("INFORMATICA loaded successfully")
    print(f"INFORMATICA rows: {len(inf_df):,}")
    print(f"INFORMATICA columns: {len(inf_df.columns):,}")
    print(f"INFORMATICA load time: {time.time() - t0:.2f}s")
    print("-----------------------------------")

    print("Initializing validator...")
    snapshot_context = SnapshotContext(
        domo_snapshot=args.domo_snapshot,
        informatica_snapshot=args.informatica_snapshot,
    )

    if args.dataset == "Prices":
        validator = PricesValidator(snapshot_context=snapshot_context)
    elif args.dataset == "Products":
        validator = ProductsValidator(snapshot_context=snapshot_context)
    else:
        validator = CurrenciesValidator(snapshot_context=snapshot_context)

    print("Validator initialized")
    print("-----------------------------------")

    print("Running validation...")
    t0 = time.time()
    run = validator.validate(domo_df, inf_df)
    print(f"Validation completed in {time.time() - t0:.2f}s")
    print("-----------------------------------")

    print("Writing reports...")
    t0 = time.time()
    evidence = write_reports(run, args.out)
    print(f"Reports written in {time.time() - t0:.2f}s")
    print("-----------------------------------")

    print("Validation finished")
    print(f"Summary: {evidence['summary']}")
    print(f"Checks: {evidence['checks']}")
    print(f"Excel: {evidence['excel']}")
    if "issue_explanations" in evidence:
        print(f"Issues: {evidence['issue_explanations']}")
    print("-----------------------------------")
    print(f"TOTAL TIME: {time.time() - start_time:.2f}s")
    print("DONE")


if __name__ == "__main__":
    main()