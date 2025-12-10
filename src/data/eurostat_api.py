"""
Eurostat Statistics API Client (JSON-stat 2.0)
2025, stable and compatible with STS_RT_M and other dissemination datasets.

Official endpoint:
    https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}

Returns JSON-stat 2.0 (NOT SDMX).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests


class EurostatAPIError(RuntimeError):
    pass


@dataclass
class EurostatAPI:
    """
    JSON-stat Eurostat API client with retry, caching, and DataFrame conversion.
    """

    base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    timeout: int = 10
    max_retries: int = 3
    backoff_factor: float = 1.5
    cache_dir: Optional[str | Path] = None

    _session: requests.Session = field(init=False, repr=False)
    _logger: logging.Logger = field(init=False, repr=False)

    # ---------------------------------------------------------
    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")

        # Logger
        self._logger = logging.getLogger(self.__class__.__name__)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

        # Cache directory
        if self.cache_dir:
            self.cache_dir = Path(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        # HTTP session
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "RetailDataIntelligence-Eurostat/2025",
            "Accept": "application/json"
        })

    # ---------------------------------------------------------
    # URL Builder
    # ---------------------------------------------------------
    def build_url(self, dataset: str, filters: Dict[str, str]) -> str:
        url = f"{self.base_url}/{dataset}"

        # always request JSON-stat format
        filters = dict(filters)
        filters["format"] = "JSON"

        qs = "&".join(f"{k}={v}" for k, v in filters.items())
        url = f"{url}?{qs}"
        return url

    # ---------------------------------------------------------
    # Fetch JSON with retry + caching
    # ---------------------------------------------------------
    def fetch_json(self, url: str, use_cache: Optional[bool]):
        if use_cache is None:
            use_cache = self.cache_dir is not None

        # Cache READ
        if use_cache:
            cached = self._read_cache(url)
            if cached:
                self._logger.info(f"Loaded from cache: {url}")
                return cached

        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=self.timeout)

                if resp.status_code == 200:
                    js = resp.json()
                    if use_cache:
                        self._write_cache(url, js)
                    return js

                if resp.status_code >= 500:
                    raise EurostatAPIError(f"Server error {resp.status_code}")

                # 400-series exception
                raise EurostatAPIError(
                    f"HTTP {resp.status_code}: {resp.text[:200]}"
                )

            except Exception as exc:
                last_exc = exc

            if attempt < self.max_retries:
                delay = self.backoff_factor ** (attempt - 1)
                self._logger.warning(f"Request failed ({last_exc}), retrying in {delay:.1f}s...")
                time.sleep(delay)

        raise EurostatAPIError(f"Failed after retries: {last_exc}")

    # ---------------------------------------------------------
    def get_dataset(self, dataset: str, filters: Dict[str, str], use_cache=True) -> pd.DataFrame:
        url = self.build_url(dataset, filters)
        self._logger.info(f"Requesting: {url}")

        js = self.fetch_json(url, use_cache)
        return self.to_dataframe(js)

    # ---------------------------------------------------------
    # JSON-stat → DataFrame
    # ---------------------------------------------------------
    def to_dataframe(self, js: Dict[str, Any]) -> pd.DataFrame:
        # JSON-stat structure
        data = js["value"]
        dims = js["dimension"]
        size = js["size"]

        dim_names = list(dims.keys())

        # Build lookup tables
        dim_indexes = {
            dim: dims[dim]["category"]["index"]
            for dim in dim_names
        }
        dim_labels = {
            dim: dims[dim]["category"]["label"]
            for dim in dim_names
        }

        # reverse index: position → key
        pos_to_key = {
            dim: {pos: key for key, pos in dim_indexes[dim].items()}
            for dim in dim_names
        }

        rows = []

        obs_id = 0

        from itertools import product

        for coords in product(*[range(n) for n in size]):
            val = data.get(str(obs_id))
            obs_id += 1

            if val is None:
                continue

            row = {}
            for dim_name, idx in zip(dim_names, coords):
                key = pos_to_key[dim_name][idx]
                row[dim_name] = dim_labels[dim_name][key]

            row["value"] = val
            rows.append(row)

        df = pd.DataFrame(rows)

        # Standardize date column
        if "time" in df.columns:
            df.rename(columns={"time": "period"}, inplace=True)

        return df

    # ---------------------------------------------------------
    # Cache
    # ---------------------------------------------------------
    def _cache_path(self, url: str) -> Path:
        h = hashlib.sha1(url.encode()).hexdigest()
        return Path(self.cache_dir) / f"{h}.json"

    def _read_cache(self, url: str):
        if not self.cache_dir:
            return None
        p = self._cache_path(url)
        return json.load(open(p, "r")) if p.exists() else None

    def _write_cache(self, url: str, js):
        if not self.cache_dir:
            return
        json.dump(js, open(self._cache_path(url), "w"))


# --------------------------------------------------------------
# MANUAL TEST
# --------------------------------------------------------------
if __name__ == "__main__":
    print("📡 Testing Eurostat STATISTICS API...\n")

    api = EurostatAPI(cache_dir=".cache/eurostat")

    try:
        df = api.get_dataset(
            dataset="STS_RT_M",
            filters={
                "geo": "DE",
                "s_adj": "NSA",
                "unit": "I15",
                "sts_activity": "G47",
                "time": "2020"
            }
        )

        print("\n✅ SUCCESS — Retrieved dataset:")
        print(df.head())
        print("Rows:", len(df))

    except Exception as e:
        print("\n❌ ERROR:")
        print(type(e).__name__, str(e))
