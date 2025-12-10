"""
Eurostat Macro Downloader — SDMX-CSV Version (Final)
----------------------------------------------------

Uses the stable SDMX 2.1 API with format=sdmx-csv to download
complete datasets. This is the officially supported way to obtain
Eurostat data in bulk as of 2024–2025.

Datasets downloaded:
- Retail Trade Index (STS_TRTU_M)
- HICP Inflation (PRC_HICP_MIDX)
- Household Final Consumption Expenditure (NAMA_10_CO3_P3)
- Consumer Confidence (EI_BSCO_M)

Output:
    data/raw/eurostat_<dataset>.csv
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"

DATASETS = {
    "rti": "STS_TRTU_M",
    "hicp": "PRC_HICP_MIDX",
    "hfce": "NAMA_10_CO3_P3",
    "cci": "EI_BSCO_M",
}

logger = logging.getLogger("EurostatSDMXDownloader")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def _download_sdmx_csv(dataset_code: str, out_path: Path) -> Path:
    """
    Download SDMX-CSV file for the full dataset.
    """

    url = f"{BASE_URL}/{dataset_code}?format=sdmx-csv"
    logger.info(f"⬇️ Downloading {dataset_code} from {url}")

    resp = requests.get(url, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(
            f"HTTP {resp.status_code}: Could not download {dataset_code}\n"
            f"Details: {resp.text[:200]}"
        )

    out_path.write_bytes(resp.content)
    logger.info(f"💾 Saved raw dataset → {out_path.resolve()}")

    return out_path


def _convert_to_csv(raw_path: Path, final_path: Path):
    """
    SDMX-CSV files are already CSV-like, but sometimes include metadata/header lines.
    This function loads using pandas with automatic delimiter detection.
    """

    logger.info(f"📤 Parsing SDMX-CSV → DataFrame: {raw_path.name}")

    df = pd.read_csv(raw_path, sep=",", low_memory=False)
    df.to_csv(final_path, index=False)

    logger.info(f"💾 Saved clean CSV → {final_path.resolve()} ({len(df):,} rows)")


# ---------------------------------------------------------------------
# MAIN FUNCTIONS
# ---------------------------------------------------------------------

def download_dataset(name: str, code: str):
    """
    Download a dataset and parse it into a clean CSV.
    """

    raw_path = RAW_DIR / f"{code}.sdmx.csv"
    clean_path = RAW_DIR / f"eurostat_{name}.csv"

    _download_sdmx_csv(code, raw_path)
    _convert_to_csv(raw_path, clean_path)

    logger.info(f"✅ Completed dataset: {code}\n")

    return clean_path


def download_all():
    """
    Download all macro datasets via SDMX-CSV.
    """

    logger.info("🚀 Starting Eurostat SDMX-CSV macro download...\n")

    for name, code in DATASETS.items():
        download_dataset(name, code)

    logger.info("🎉 All Eurostat macro datasets downloaded successfully.\n")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    download_all()
