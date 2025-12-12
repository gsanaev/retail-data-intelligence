"""
ecb_financial.py
----------------

Downloads ECB financial series using the ECB Data Portal API (SDMX 2.1),
restricted to the project analysis window.

Outputs (monthly, clean CSVs):

- data/raw/ecb_yields.csv         (AAA gov bond yields: 1Y, 2Y, 5Y, 10Y)
- data/raw/ecb_policy_rates.csv   (DFR, MRO, MLF)
- data/raw/ecb_estr.csv           (€STR)
- data/raw/ecb_fx.csv             (USD/EUR, GBP/EUR)
"""

from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path
from typing import Dict

import pandas as pd
import requests

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

BASE_URL = "https://data-api.ecb.europa.eu/service/data"

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

START_PERIOD = "2010-01"
END_PERIOD = "2024-12"

TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOG = logging.getLogger("ecb_financial")


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def download_csv(url: str, name: str) -> pd.DataFrame:
    LOG.info(f"⬇️ Requesting {name}")
    resp = requests.get(url, timeout=TIMEOUT)

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {name}\nURL: {url}")

    return pd.read_csv(StringIO(resp.text))


def to_month(df: pd.DataFrame) -> pd.DataFrame:
    df["month"] = pd.to_datetime(df["TIME_PERIOD"], errors="coerce") \
                    .dt.to_period("M").astype(str)
    return df


# ---------------------------------------------------------------------
# 1. AAA GOVERNMENT BOND YIELDS (MONTHLY)
# ---------------------------------------------------------------------

def download_aaa_yields() -> None:
    """
    AAA euro area government bond yields:
    1Y, 2Y, 5Y, 10Y
    """

    urls: Dict[str, str] = {
        "yc_1y": f"{BASE_URL}/FM/M.U2.EUR.GVT.AAA.1Y"
                 f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
        "yc_2y": f"{BASE_URL}/FM/M.U2.EUR.GVT.AAA.2Y"
                 f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
        "yc_5y": f"{BASE_URL}/FM/M.U2.EUR.GVT.AAA.5Y"
                 f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
        "yc_10y": f"{BASE_URL}/FM/M.U2.EUR.GVT.AAA.10Y"
                  f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
    }

    frames = []

    for col, url in urls.items():
        df = download_csv(url, col)
        df = to_month(df)
        frames.append(
            df[["month", "OBS_VALUE"]].rename(columns={"OBS_VALUE": col})
        )

    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="month", how="outer")

    out.sort_values("month").to_csv(DATA_DIR / "ecb_yields.csv", index=False)
    LOG.info(f"💾 Saved ecb_yields.csv ({len(out):,} rows)")


# ---------------------------------------------------------------------
# 2. ECB POLICY RATES (MONTHLY)
# ---------------------------------------------------------------------

def download_policy_rates() -> None:
    urls = {
        "dfr_rate": f"{BASE_URL}/FM/M.U2.EUR.DFR"
                    f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
        "mro_rate": f"{BASE_URL}/FM/M.U2.EUR.MMR"
                    f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
        "mlf_rate": f"{BASE_URL}/FM/M.U2.EUR.MLF"
                    f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
    }

    frames = []

    for col, url in urls.items():
        df = download_csv(url, col)
        df = to_month(df)
        frames.append(
            df[["month", "OBS_VALUE"]].rename(columns={"OBS_VALUE": col})
        )

    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="month", how="outer")

    out.sort_values("month").to_csv(DATA_DIR / "ecb_policy_rates.csv", index=False)
    LOG.info(f"💾 Saved ecb_policy_rates.csv ({len(out):,} rows)")


# ---------------------------------------------------------------------
# 3. €STR (MONTHLY)
# ---------------------------------------------------------------------

def download_estr() -> None:
    url = (
        f"{BASE_URL}/EST/M.U2.EUR.STR"
        f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}"
    )

    df = download_csv(url, "estr")
    df = to_month(df)

    out = df[["month", "OBS_VALUE"]].rename(columns={"OBS_VALUE": "estr_rate"})
    out.sort_values("month").to_csv(DATA_DIR / "ecb_estr.csv", index=False)

    LOG.info(f"💾 Saved ecb_estr.csv ({len(out):,} rows)")


# ---------------------------------------------------------------------
# 4. FX RATES (MONTHLY)
# ---------------------------------------------------------------------

def download_fx() -> None:
    urls = {
        "exr_usd_eur": f"{BASE_URL}/EXR/M.USD.EUR.SP00.A"
                       f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
        "exr_gbp_eur": f"{BASE_URL}/EXR/M.GBP.EUR.SP00.A"
                       f"?format=csvdata&startPeriod={START_PERIOD}&endPeriod={END_PERIOD}",
    }

    frames = []

    for col, url in urls.items():
        df = download_csv(url, col)
        df = to_month(df)
        frames.append(
            df[["month", "OBS_VALUE"]].rename(columns={"OBS_VALUE": col})
        )

    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="month", how="outer")

    out.sort_values("month").to_csv(DATA_DIR / "ecb_fx.csv", index=False)
    LOG.info(f"💾 Saved ecb_fx.csv ({len(out):,} rows)")


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main() -> None:
    LOG.info("🚀 Starting ECB financial data download\n")

    download_aaa_yields()
    download_policy_rates()
    download_estr()
    download_fx()

    LOG.info("\n🏁 ECB financial data download finished successfully.")


if __name__ == "__main__":
    main()
