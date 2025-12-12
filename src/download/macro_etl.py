"""
Macro ETL Pipeline: Eurostat + FRED (EA7 Panel)
----------------------------------------------

Builds a monthly macro–financial panel for 7 core euro area economies:

    EA7 = ["DE", "FR", "IT", "ES", "NL", "AT", "FI"]

Analysis window:
    2010-01 → 2024-12

Eurostat input (SDMX-CSV in data/raw):
    - STS_TRTU_M.sdmx.csv     → Retail Trade Index (RTI)
    - PRC_HICP_MIDX.sdmx.csv  → HICP (CP00)
    - NAMA_10_CO3_P3.sdmx.csv → HFCE (annual → expanded monthly)
    - EI_BSCO_M.sdmx.csv      → Consumer Confidence

FRED input (monthly):
    - fred_financial.csv     → global financial conditions

Final output:
    data/processed/macro_panel.csv
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import pandas as pd

# ---------------------------------------------------------------------
# PATHS & CONSTANTS
# ---------------------------------------------------------------------

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)

OUT_PATH = PROC_DIR / "macro_panel.csv"

EA7: List[str] = ["DE", "FR", "IT", "ES", "NL", "AT", "FI"]

START_MONTH = "2010-01"
END_MONTH = "2024-12"

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------

LOG = logging.getLogger("macro_etl")
if not LOG.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def _std_country(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()

def _to_month(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.to_period("M").astype(str)

def _filter_window(df: pd.DataFrame, col: str = "month") -> pd.DataFrame:
    return df[(df[col] >= START_MONTH) & (df[col] <= END_MONTH)].copy()

def _require(df: pd.DataFrame, cols: set, name: str):
    missing = cols - set(df.columns)
    if missing:
        raise KeyError(f"{name} missing columns: {missing}")

# ---------------------------------------------------------------------
# EUROSTAT LOADERS
# ---------------------------------------------------------------------

def load_rti() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "STS_TRTU_M.sdmx.csv", low_memory=False)
    _require(df, {"TIME_PERIOD","OBS_VALUE","geo","nace_r2","s_adj","unit","indic_bt","freq"}, "RTI")

    df["country"] = _std_country(df["geo"])
    df = df[df["country"].isin(EA7)]
    df = df[(df["freq"] == "M") & (df["indic_bt"] == "VOL_SLS")]
    df = df[(df["nace_r2"] == "G47") & (df["unit"] == "I15")]

    if "SCA" in df["s_adj"].unique():
        df = df[df["s_adj"] == "SCA"]
    elif "SA" in df["s_adj"].unique():
        df = df[df["s_adj"] == "SA"]
    else:
        df = df[df["s_adj"] == "NSA"]

    df["month"] = _to_month(df["TIME_PERIOD"])
    df["rti_index"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

    out = _filter_window(df[["country","month","rti_index"]].dropna())
    LOG.info(f"RTI rows: {len(out):,}")
    return out

def load_hicp() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "PRC_HICP_MIDX.sdmx.csv", low_memory=False)
    _require(df, {"TIME_PERIOD","OBS_VALUE","geo","coicop","freq"}, "HICP")

    df["country"] = _std_country(df["geo"])
    df = df[df["country"].isin(EA7)]
    df = df[(df["freq"] == "M") & (df["coicop"] == "CP00")]

    df["month"] = _to_month(df["TIME_PERIOD"])
    df["hicp_index"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

    out = _filter_window(df[["country","month","hicp_index"]].dropna())
    LOG.info(f"HICP rows: {len(out):,}")
    return out

def load_hfce() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "NAMA_10_CO3_P3.sdmx.csv", low_memory=False)
    _require(df, {"TIME_PERIOD","OBS_VALUE","geo","coicop","freq"}, "HFCE")

    df["country"] = _std_country(df["geo"])
    df = df[df["country"].isin(EA7)]
    df = df[df["freq"] == "A"]

    if "CP00" in df["coicop"].unique():
        df = df[df["coicop"] == "CP00"]

    df["year"] = pd.to_datetime(df["TIME_PERIOD"], errors="coerce").dt.year
    df = df[(df["year"] >= 2010) & (df["year"] <= 2024)]

    base = df[["country","year","OBS_VALUE"]]
    expanded = base.loc[base.index.repeat(12)].copy()
    expanded["month"] = (
        expanded["year"].astype(str)
        + "-"
        + (expanded.groupby(["country","year"]).cumcount()+1).astype(str).str.zfill(2)
    )

    expanded["hfce"] = pd.to_numeric(expanded["OBS_VALUE"], errors="coerce")
    out = _filter_window(expanded[["country","month","hfce"]].dropna())
    LOG.info(f"HFCE rows: {len(out):,}")
    return out

def load_cci() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "EI_BSCO_M.sdmx.csv", low_memory=False)
    _require(df, {"TIME_PERIOD","OBS_VALUE","geo","freq","indic","s_adj"}, "CCI")

    df["country"] = _std_country(df["geo"])
    df = df[df["country"].isin(EA7)]
    df = df[(df["freq"] == "M") & (df["indic"] == "BS-CSMCI")]

    if "SA" in df["s_adj"].unique():
        df = df[df["s_adj"] == "SA"]

    df["month"] = _to_month(df["TIME_PERIOD"])
    df["cci"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

    out = (
        df[["country","month","cci"]]
        .dropna()
        .groupby(["country","month"], as_index=False)
        .mean()
    )

    out = _filter_window(out)
    LOG.info(f"CCI rows: {len(out):,}")
    return out

# ---------------------------------------------------------------------
# FRED LOADER (GLOBAL)
# ---------------------------------------------------------------------

def load_fred() -> pd.DataFrame:
    path = RAW_DIR / "fred_financial.csv"
    if not path.exists():
        LOG.warning("FRED file missing → skipping.")
        return pd.DataFrame()

    df = pd.read_csv(path)
    if "month" not in df.columns:
        raise KeyError("fred_financial.csv missing 'month' column")

    df["month"] = df["month"].astype(str)
    df = _filter_window(df)

    LOG.info(f"FRED rows: {len(df):,}")
    return df

# ---------------------------------------------------------------------
# MASTER PIPELINE
# ---------------------------------------------------------------------

def build_macro_panel() -> pd.DataFrame:
    LOG.info("📊 Building EA7 macro–financial panel...\n")

    panel = load_rti()
    panel = panel.merge(load_hicp(), on=["country","month"], how="left")
    panel = panel.merge(load_hfce(), on=["country","month"], how="left")
    panel = panel.merge(load_cci(), on=["country","month"], how="left")

    LOG.info("🔗 Merging global financial data (FRED)...")
    fred = load_fred()
    if not fred.empty:
        panel = panel.merge(fred, on="month", how="left")

    panel = (
        panel.drop_duplicates(["country","month"])
        .sort_values(["country","month"])
        .reset_index(drop=True)
    )

    panel.to_csv(OUT_PATH, index=False)
    LOG.info(f"💾 Saved macro panel → {OUT_PATH} ({len(panel):,} rows)\n")

    return panel

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    build_macro_panel()
