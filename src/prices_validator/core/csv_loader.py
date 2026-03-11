from __future__ import annotations

import pandas as pd


def load_csv_raw(path: str, delimiter: str = ",", encoding: str = "utf-8") -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep=delimiter,
        dtype=str,
        encoding=encoding,
        keep_default_na=False,
        na_filter=False,
    )
