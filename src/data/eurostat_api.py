"""
Eurostat SDMX-JSON API Client (2025, WORKING)

Uses the new Eurostat Explorer SDMX endpoint:
    https://ec.europa.eu/eurostat/api/explorer/sdmx/data/{dataset}/{filters}

This replaces the deprecated /api/discover endpoints.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


class EurostatAPIError(RuntimeError):
    pass


@dataclass
class EurostatAPI:
    """
    Final production-ready Eurostat SDMX client using the new Explorer endpoints.
    """

    base_url: str = "https://ec.europa.eu/eurostat/api/explorer/sdmx/data"
    timeout: int = 10
    max_retries: int = 3
    backoff_factor: float = 1.5
    cache_dir: Optional[str | Path] = None

    _session: requests.Session = field(init=False, repr=False)
    _logger: logging.Logger = field(init=False, repr=False)

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")

        # Logger
        self._logger = logging.getLogger(self.__class__.__name__)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            )
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

        # Cache directory
        if self.cache_dir:
            self.cache_dir = Path(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Session
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "EurostatExplorerClient/2025", "Accept": "application/json"}
        )

    # ------------------------------------------------------------------
    # Build SDMX REST URL
    # ------------------------------------------------------------------
    def build_url(self, dataset: str, filters: List[str], params: Dict[str, str] | None):
        filter_path = ".".join(filters)
        url = f"{self.base_url}/{dataset}/{filter_path}"

        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url += f"?{qs}"

        return url

    # ------------------------------------------------------------------
    # Fetch JSON with retry + cache
    # ------------------------------------------------------------------
    def fetch_json(self, url: str, use_cache: Optional[bool]):
        if use_cache is None:
            use_cache = self.cache_dir is not None

        # Cache read
        if use_cache:
            js = self._read_cache(url)
            if js:
                return js

        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=self.timeout)

                if resp.status_code == 200:
                    js = resp.json()
                    if use_cache:
                        self._write_cache(url, js)
                    return js

                if 500 <= resp.status_code < 600:
                    raise EurostatAPIError(f"Server {resp.status_code}")

                raise EurostatAPIError(f"HTTP {resp.status_code}: {resp.text[:200]}")

            except Exception as exc:
                last_exc = exc

            if attempt < self.max_retries:
                delay = self.backoff_factor ** (attempt - 1)
                self._logger.info(f"Retrying in {delay:.1f}s...")
                time.sleep(delay)

        raise EurostatAPIError(f"Failed after retries: {last_exc}")

    # ------------------------------------------------------------------
    # Public method
    # ------------------------------------------------------------------
    def get_dataset(
        self, dataset: str, filters: List[str], params=None, use_cache=None
    ) -> pd.DataFrame:

        url = self.build_url(dataset, filters, params)
        js = self.fetch_json(url, use_cache)
        return self.to_dataframe(js)

    # ------------------------------------------------------------------
    # Convert SDMX JSON → DataFrame
    # ------------------------------------------------------------------
    def to_dataframe(self, js: Dict[str, Any]) -> pd.DataFrame:
        dims = js["structure"]["dimensions"]["observation"]
        dim_names = [d["id"] for d in dims]

        obs = js["dataSets"][0]["observations"]
        dim_vals = {d["id"]: d["values"] for d in dims}

        rows = []
        for key, val in obs.items():
            idxs = list(map(int, key.split(":")))
            value = val[0] if val else None

            row = {"value": value}
            for dim, idx in zip(dim_names, idxs):
                row[dim] = dim_vals[dim][idx]["id"]
            rows.append(row)

        df = pd.DataFrame(rows)
        df.rename(columns={"TIME_PERIOD": "period"}, inplace=True)
        return df

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------
    def _cache_path(self, url: str) -> Path:
        return Path(self.cache_dir) / (hashlib.sha1(url.encode()).hexdigest() + ".json")

    def _read_cache(self, url: str):
        if not self.cache_dir:
            return None
        p = self._cache_path(url)
        if not p.exists():
            return None
        return json.load(open(p, "r"))

    def _write_cache(self, url: str, data):
        if not self.cache_dir:
            return
        json.dump(data, open(self._cache_path(url), "w"))


# ----------------------------------------------------------------------
# Manual Test
# ----------------------------------------------------------------------
if __name__ == "__main__":
    api = EurostatAPI(cache_dir=".cache/eurostat")

    df = api.get_dataset(
        "STS_RT_M",
        ["DE", "NSA", "I15", "G47"],  # Ordered SDMX dimensions
        params={"detail": "trunc"},
    )

    print(df.head())
    print("Rows:", len(df))
