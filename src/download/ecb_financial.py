"""
ecb_financial.py
Download ECB yield curve, policy rates, FX, and €STR using the NEW SDMX API syntax.
"""

import pandas as pd
import requests
from pathlib import Path
from io import StringIO
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

DATA_DIR = Path("data/raw")


def download_sdmx_csv(url: str, name: str) -> pd.DataFrame | None:
    """Download SDMX-CSV and return DataFrame."""
    logging.info(f"Requesting: {url}")

    r = requests.get(url)
    if r.status_code != 200:
        logging.error(f"❌ HTTP {r.status_code} for {name}")
        return None

    try:
        df = pd.read_csv(StringIO(r.text))
        logging.info(f"Loaded {name}: {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"❌ Failed to parse CSV for {name}: {e}")
        return None


# --------------------- Working ECB URLs (shortened with date filter) ---------------------

BASE_START = "&startPeriod=1990-01"

YIELD_CURVE = {
    "yc_1y": f"https://data-api.ecb.europa.eu/service/data/YC"
             f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
             f"&filter=REF_AREA:U2,MATURITY:SR_1Y{BASE_START}",

    "yc_2y": f"https://data-api.ecb.europa.eu/service/data/YC"
             f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
             f"&filter=REF_AREA:U2,MATURITY:SR_2Y{BASE_START}",

    "yc_5y": f"https://data-api.ecb.europa.eu/service/data/YC"
             f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
             f"&filter=REF_AREA:U2,MATURITY:SR_5Y{BASE_START}",

    "yc_10y": f"https://data-api.ecb.europa.eu/service/data/YC"
              f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
              f"&filter=REF_AREA:U2,MATURITY:SR_10Y{BASE_START}",
}

POLICY_RATES = {
    "deposit_facility": f"https://data-api.ecb.europa.eu/service/data/FM"
                        f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
                        f"&filter=REF_AREA:U2,KEY_FIGURE:DFR{BASE_START}",

    "mro": f"https://data-api.ecb.europa.eu/service/data/FM"
           f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
           f"&filter=REF_AREA:U2,KEY_FIGURE:MMR{BASE_START}",

    "marginal_lending": f"https://data-api.ecb.europa.eu/service/data/FM"
                        f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE"
                        f"&filter=REF_AREA:U2,KEY_FIGURE:MLF{BASE_START}",
}

ESTR = {
    "estr": f"https://data-api.ecb.europa.eu/service/data/EST"
            f"?format=sdmx-csv&select=TIME_PERIOD,OBS_VALUE{BASE_START}"
}

FX = {
    "usd": "https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata",
    "gbp": "https://data-api.ecb.europa.eu/service/data/EXR/M.GBP.EUR.SP00.A?format=csvdata",
}

# ------------------------------------------------------------------------------
# Download main
# ------------------------------------------------------------------------------

def main():
    logging.info("🚀 Starting ECB financial data download...")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # YIELD CURVE
    yield_frames = []
    for name, url in YIELD_CURVE.items():
        df = download_sdmx_csv(url, name)
        if df is not None:
            df["series"] = name
            yield_frames.append(df)

    if yield_frames:
        yc = pd.concat(yield_frames, ignore_index=True)
        yc.to_csv(DATA_DIR / "ecb_yields.csv", index=False)
        logging.info(f"💾 Saved ecb_yields.csv ({len(yc)} rows)")

    # POLICY RATES
    rate_frames = []
    for name, url in POLICY_RATES.items():
        df = download_sdmx_csv(url, name)
        if df is not None:
            df["series"] = name
            rate_frames.append(df)

    if rate_frames:
        pr = pd.concat(rate_frames, ignore_index=True)
        pr.to_csv(DATA_DIR / "ecb_policy_rates.csv", index=False)
        logging.info(f"💾 Saved ecb_policy_rates.csv ({len(pr)} rows)")

    # €STR
    for name, url in ESTR.items():
        df = download_sdmx_csv(url, name)
        if df is not None:
            df.to_csv(DATA_DIR / "ecb_estr.csv", index=False)
            logging.info(f"💾 Saved ecb_estr.csv ({len(df)} rows)")

    # FX
    fx_frames = []
    for name, url in FX.items():
        df = download_sdmx_csv(url, name)
        if df is not None:
            df["currency"] = name
            fx_frames.append(df)

    if fx_frames:
        fx = pd.concat(fx_frames, ignore_index=True)
        fx.to_csv(DATA_DIR / "ecb_fx.csv", index=False)
        logging.info(f"💾 Saved ecb_fx.csv ({len(fx)} rows)")

    logging.info("🏁 ECB financial download finished.")


if __name__ == "__main__":
    main()
