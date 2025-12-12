"""
fred_financial.py
-----------------
Download macro-financial indicators from FRED using direct CSV endpoints.
No pandas_datareader. Python 3.12 compatible.

Outputs:
- data/raw/fred_financial.csv
"""

from __future__ import annotations

import logging
from pathlib import Path
from io import StringIO

import pandas as pd
import requests

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_PATH = DATA_DIR / "fred_financial.csv"

START_DATE = "2010-01-01"
END_DATE = "2024-12-31"

# FRED series (monthly)
FRED_SERIES = {
    "us_cpi": "CPIAUCSL",
    "us_unemployment": "UNRATE",
    "us_fed_funds": "FEDFUNDS",
    "us_industrial_prod": "INDPRO",
}

BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------

LOG = logging.getLogger("fred_financial")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    LOG.addHandler(h)
    LOG.setLevel(logging.INFO)

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------


def download_series(name: str, series_id: str) -> pd.DataFrame:
    LOG.info(f"⬇️ Downloading FRED series: {name} ({series_id})")

    params = {
        "id": series_id,
        "cosd": START_DATE,
        "coed": END_DATE,
    }

    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))
    df.columns = ["date", name]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[name] = pd.to_numeric(df[name], errors="coerce")

    df = df.dropna()
    df["month"] = df["date"].dt.to_period("M").astype(str) # type: ignore

    return df[["month", name]]


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------


def main() -> None:
    LOG.info("🚀 Starting FRED financial download")

    frames = []
    for name, series in FRED_SERIES.items():
        df = download_series(name, series)
        frames.append(df)

    panel = frames[0]
    for df in frames[1:]:
        panel = panel.merge(df, on="month", how="outer")

    panel = panel.sort_values("month").reset_index(drop=True)
    panel.to_csv(OUT_PATH, index=False)

    LOG.info(f"💾 Saved FRED data → {OUT_PATH} ({len(panel)} rows)")
    LOG.info("🏁 FRED download finished successfully")


if __name__ == "__main__":
    main()
