"""
Eurostat Macro Downloader — SDMX-CSV (Raw Only)
-----------------------------------------------

Downloads full Eurostat macro datasets using the official
SDMX 2.1 CSV endpoint.

Outputs (raw, untouched):
    - data/raw/STS_TRTU_M.sdmx.csv
    - data/raw/PRC_HICP_MIDX.sdmx.csv
    - data/raw/NAMA_10_CO3_P3.sdmx.csv
    - data/raw/EI_BSCO_M.sdmx.csv
"""

from __future__ import annotations

import logging
from pathlib import Path
import requests

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"

DATASETS = {
    "STS_TRTU_M": "Retail Trade Index",
    "PRC_HICP_MIDX": "HICP Inflation",
    "NAMA_10_CO3_P3": "Household Consumption",
    "EI_BSCO_M": "Consumer Confidence",
}

LOG = logging.getLogger("eurostat_macro")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    LOG.addHandler(h)
    LOG.setLevel(logging.INFO)

# ---------------------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------------------

def download_dataset(code: str) -> None:
    url = f"{BASE_URL}/{code}?format=sdmx-csv"
    out_path = RAW_DIR / f"{code}.sdmx.csv"

    LOG.info(f"⬇️ Downloading {code}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    out_path.write_bytes(r.content)
    LOG.info(f"💾 Saved → {out_path}")

def main() -> None:
    LOG.info("🚀 Starting Eurostat macro download\n")

    for code in DATASETS:
        download_dataset(code)

    LOG.info("\n🎉 Eurostat macro download finished successfully")

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()
