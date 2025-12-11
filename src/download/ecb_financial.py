"""
ecb_financial.py
Download ECB yield curve, policy rates, FX, and €STR using the NEW SDMX API syntax.
"""

from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
import requests

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOG = logging.getLogger("ecb_financial")

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Helper: robust SDMX-CSV parsing
# ---------------------------------------------------------------------


def _parse_sdmx_csv(text: str, name: str) -> Optional[pd.DataFrame]:
    """
    ECB SDMX-CSV responses often contain metadata before the actual header.
    We:
      - split into lines
      - find the first line that starts with TIME_PERIOD
      - use that as the header, parse the rest as CSV
    """
    lines = text.splitlines()
    header_idx = None

    for i, line in enumerate(lines):
        if line.startswith("TIME_PERIOD") or line.startswith("TIME_PERIOD,") or line.startswith("TIME_PERIOD;"):
            header_idx = i
            break

    if header_idx is None:
        LOG.error(f"❌ {name}: Could not find TIME_PERIOD header in SDMX-CSV.")
        return None

    csv_body = "\n".join(lines[header_idx:])

    header_line = lines[header_idx]
    if ";" in header_line and "," not in header_line:
        sep = ";"
    else:
        sep = ","

    try:
        df = pd.read_csv(StringIO(csv_body), sep=sep)
        LOG.info(f"Parsed SDMX-CSV for {name}: {len(df)} rows, cols={df.columns.tolist()}")
        return df
    except Exception as e:
        LOG.error(f"❌ Failed to parse CSV body for {name}: {e}")
        return None


def download_sdmx_csv(url: str, name: str) -> Optional[pd.DataFrame]:
    """Download SDMX-CSV and return DataFrame using robust parser."""
    LOG.info(f"Requesting: {url}")
    resp = requests.get(url, timeout=60)

    if resp.status_code != 200:
        LOG.error(f"❌ HTTP {resp.status_code} for {name}")
        return None

    return _parse_sdmx_csv(resp.text, name)


# ---------------------------------------------------------------------
# URLs (with date filters to keep downloads reasonable)
# ---------------------------------------------------------------------

BASE_START = "&startPeriod=2010-01"

YIELD_CURVE: Dict[str, str] = {
    "yc_1y": (
        "https://data-api.ecb.europa.eu/service/data/YC"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,MATURITY:SR_1Y" + BASE_START
    ),
    "yc_2y": (
        "https://data-api.ecb.europa.eu/service/data/YC"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,MATURITY:SR_2Y" + BASE_START
    ),
    "yc_5y": (
        "https://data-api.ecb.europa.eu/service/data/YC"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,MATURITY:SR_5Y" + BASE_START
    ),
    "yc_10y": (
        "https://data-api.ecb.europa.eu/service/data/YC"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,MATURITY:SR_10Y" + BASE_START
    ),
}

POLICY_RATES: Dict[str, str] = {
    "deposit_facility": (
        "https://data-api.ecb.europa.eu/service/data/FM"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,KEY_FIGURE:DFR" + BASE_START
    ),
    "mro": (
        "https://data-api.ecb.europa.eu/service/data/FM"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,KEY_FIGURE:MMR" + BASE_START
    ),
    "marginal_lending": (
        "https://data-api.ecb.europa.eu/service/data/FM"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
        "&filter=REF_AREA:U2,KEY_FIGURE:MLF" + BASE_START
    ),
}

ESTR: Dict[str, str] = {
    "estr": (
        "https://data-api.ecb.europa.eu/service/data/EST"
        "?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE" + BASE_START
    )
}

# FX still uses csvdata, but response is also CSV-ish. We can reuse parser.
FX: Dict[str, str] = {
    "usd": "https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata",
    "gbp": "https://data-api.ecb.europa.eu/service/data/EXR/M.GBP.EUR.SP00.A?format=csvdata",
}


# ---------------------------------------------------------------------
# Download main
# ---------------------------------------------------------------------


def main() -> None:
    LOG.info("🚀 Starting ECB financial data download...")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # --------- Yield curve ---------
    yield_frames = []
    for name, url in YIELD_CURVE.items():
        df = download_sdmx_csv(url, name)
        if df is not None and not df.empty:
            df["series"] = name
            yield_frames.append(df)

    if yield_frames:
        yc = pd.concat(yield_frames, ignore_index=True)
        yc.to_csv(DATA_DIR / "ecb_yields.csv", index=False)
        LOG.info(f"💾 Saved ecb_yields.csv ({len(yc)} rows)")
    else:
        LOG.warning("⚠️ No yield curve data downloaded.")

    # --------- Policy rates ---------
    rate_frames = []
    for name, url in POLICY_RATES.items():
        df = download_sdmx_csv(url, name)
        if df is not None and not df.empty:
            df["series"] = name
            rate_frames.append(df)

    if rate_frames:
        pr = pd.concat(rate_frames, ignore_index=True)
        pr.to_csv(DATA_DIR / "ecb_policy_rates.csv", index=False)
        LOG.info(f"💾 Saved ecb_policy_rates.csv ({len(pr)} rows)")
    else:
        LOG.warning("⚠️ No policy rate data downloaded.")

    # --------- €STR ---------
    for name, url in ESTR.items():
        df = download_sdmx_csv(url, name)
        if df is not None and not df.empty:
            df.to_csv(DATA_DIR / "ecb_estr.csv", index=False)
            LOG.info(f"💾 Saved ecb_estr.csv ({len(df)} rows)")
        else:
            LOG.warning("⚠️ No €STR data downloaded.")

    # --------- FX ---------
    fx_frames = []
    for name, url in FX.items():
        df = download_sdmx_csv(url, name)
        if df is not None and not df.empty:
            df["currency"] = name
            fx_frames.append(df)

    if fx_frames:
        fx = pd.concat(fx_frames, ignore_index=True)
        fx.to_csv(DATA_DIR / "ecb_fx.csv", index=False)
        LOG.info(f"💾 Saved ecb_fx.csv ({len(fx)} rows)")
    else:
        LOG.warning("⚠️ No FX data downloaded.")

    LOG.info("🏁 ECB financial download finished.")


if __name__ == "__main__":
    main()
