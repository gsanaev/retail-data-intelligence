"""
Eurostat SDMX JSON API client (2025-ready).

Key features
------------
- Handles Eurostat SDMX JSON endpoint:
    https://ec.europa.eu/eurostat/api/discover/sdmx
- Retries on transient errors (network / 5xx)
- Optional on-disk caching keyed by URL
- Converts SDMX JSON → tidy pandas.DataFrame
- Helper to inspect dimensions / codes

Example
-------
    from src.data.eurostat_api import EurostatAPI

    api = EurostatAPI(cache_dir=".cache/eurostat")
    df = api.get_dataset(
        "STS_RT_M",
        {
            "geo": "DE",
            "s_adj": "NSA",
            "unit": "I15",
            "sts_activity": "G47",
        },
    )
    print(df.head())
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Union

import pandas as pd
import requests


FilterValue = Union[str, Iterable[str]]


class EurostatAPIError(RuntimeError):
    """Custom exception for Eurostat API-related errors."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class EurostatAPI:
    """
    Eurostat SDMX JSON API client.

    Parameters
    ----------
    base_url:
        Base URL for the SDMX API.
    timeout:
        Request timeout in seconds.
    max_retries:
        Number of retries for transient errors (network / 5xx).
    backoff_factor:
        Exponential backoff factor for retries (in seconds).
    cache_dir:
        If provided, responses are cached on disk keyed by URL.
        Useful for development and reproducibility.

    Notes
    -----
    - This client is *read-only* and safe to use in analytics pipelines.
    - It is intentionally lightweight and avoids heavy dependencies.
    """

    base_url: str = "https://ec.europa.eu/eurostat/api/discover/sdmx"
    timeout: int = 10
    max_retries: int = 3
    backoff_factor: float = 1.5
    cache_dir: Optional[Union[str, Path]] = None

    # internal fields
    _session: requests.Session = field(init=False, repr=False)
    _logger: logging.Logger = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Normalize base URL
        self.base_url = self.base_url.rstrip("/")

        # Setup cache directory if enabled
        if self.cache_dir is not None:
            self.cache_dir = Path(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self._logger = logging.getLogger(self.__class__.__name__)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

        # Setup persistent session with browser-like headers
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://ec.europa.eu/eurostat/",
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_dataset(
        self,
        dataset_code: str,
        filters: Mapping[str, FilterValue],
        use_cache: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        Fetch dataset and return as tidy DataFrame.

        Parameters
        ----------
        dataset_code:
            Eurostat table code, e.g. "STS_RT_M".
        filters:
            Mapping from dimension ID to code(s), e.g.:
                {"geo": "DE", "s_adj": "NSA", "unit": "I15"}
            You can also pass multiple codes as a list/tuple:
                {"geo": ["DE", "FR"], ...}
        use_cache:
            Override cache usage. If None, inferred from cache_dir.

        Returns
        -------
        pandas.DataFrame
            Tidy DataFrame with columns for each dimension and a "value" column.
        """
        url = self.build_url(dataset_code, filters)
        json_data = self.fetch_json(url, use_cache=use_cache)
        return self.to_dataframe(json_data)

    def get_raw_dataset(
        self,
        dataset_code: str,
        filters: Mapping[str, FilterValue],
        use_cache: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Fetch dataset and return raw SDMX JSON.

        Useful when you want to inspect structure / metadata.
        """
        url = self.build_url(dataset_code, filters)
        return self.fetch_json(url, use_cache=use_cache)

    # ------------------------------------------------------------------
    # URL building
    # ------------------------------------------------------------------
    def build_url(
        self,
        dataset_code: str,
        filters: Mapping[str, FilterValue],
    ) -> str:
        """
        Build SDMX JSON URL with filters.

        Eurostat supports comma-separated codes for dimensions.
        Example:
            geo=DE,FR

        This method converts list-like values to that format.
        """
        query_parts = [
            f"table={dataset_code}",
            "format=JSON",
        ]

        for key, value in filters.items():
            if value is None:
                continue

            if isinstance(value, str):
                codes_str = value
            else:
                values_list = list(value)
                if not values_list:
                    continue
                # Keep order as provided
                codes_str = ",".join(str(v) for v in values_list)

            query_parts.append(f"{key}={codes_str}")

        query_string = "&".join(query_parts)
        return f"{self.base_url}?{query_string}"

    # ------------------------------------------------------------------
    # HTTP + caching + retries
    # ------------------------------------------------------------------
    def fetch_json(
        self,
        url: str,
        use_cache: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Perform a GET request with retries and optional caching.

        Parameters
        ----------
        url:
            Fully qualified SDMX URL.
        use_cache:
            If True and cache_dir is set, reads/writes from disk cache.

        Raises
        ------
        EurostatAPIError
            For non-200 responses or unexpected payloads.
        requests.RequestException
            For lower-level network issues (after retries).
        """
        if use_cache is None:
            use_cache = self.cache_dir is not None

        # Try cache first
        if use_cache and self.cache_dir is not None:
            cached = self._read_from_cache(url)
            if cached is not None:
                self._logger.info("Cache hit for URL: %s", url)
                return cached

        self._logger.info("Fetching URL: %s", url)

        last_exc: Optional[BaseException] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.get(url, timeout=self.timeout)

                # Handle HTTP status codes
                if 500 <= response.status_code < 600:
                    # Server-side issues → retriable
                    msg = (
                        f"Eurostat server error {response.status_code}: "
                        f"{response.text[:200]}"
                    )
                    self._logger.warning(msg)
                    raise EurostatAPIError(msg, status_code=response.status_code)

                if response.status_code != 200:
                    # Client-side or other errors → do not retry
                    msg = (
                        f"Eurostat API returned status {response.status_code}: "
                        f"{response.text[:200]}"
                    )
                    raise EurostatAPIError(msg, status_code=response.status_code)

                # Parse JSON
                data = response.json()

                if "structure" not in data or "dataSets" not in data:
                    raise EurostatAPIError(
                        f"Unexpected response format: keys={list(data.keys())}"
                    )

                # Persist to cache
                if use_cache and self.cache_dir is not None:
                    self._write_to_cache(url, data)

                return data

            except EurostatAPIError as exc:
                # For server errors we may retry, others bubble up
                last_exc = exc
                if isinstance(exc, EurostatAPIError) and (
                    exc.status_code is None
                    or exc.status_code < 500
                    or exc.status_code >= 600
                ):
                    # Non-retriable error
                    self._logger.error("Non-retriable EurostatAPIError: %s", exc)
                    raise

            except requests.RequestException as exc:
                last_exc = exc
                self._logger.warning("Network error: %s", exc)

            # Retry path
            if attempt < self.max_retries:
                sleep_time = self.backoff_factor ** (attempt - 1)
                self._logger.info(
                    "Retrying (attempt %d/%d) in %.1f seconds...",
                    attempt + 1,
                    self.max_retries,
                    sleep_time,
                )
                time.sleep(sleep_time)
            else:
                self._logger.error("Exceeded max retries (%d)", self.max_retries)

        # If we reach here, all retries failed
        if isinstance(last_exc, EurostatAPIError):
            raise last_exc
        elif last_exc is not None:
            raise EurostatAPIError(f"Request failed: {last_exc}") from last_exc
        else:
            raise EurostatAPIError("Request failed for unknown reasons")

    # ------------------------------------------------------------------
    # SDMX JSON → tidy DataFrame
    # ------------------------------------------------------------------
    def to_dataframe(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Convert SDMX JSON into a tidy pandas DataFrame.

        Returns a DataFrame with:
            - One column per dimension (e.g. geo, unit, s_adj, TIME_PERIOD)
            - Column 'value' for the observation
        """

        # ---- Extract dimensions ----
        try:
            dims = json_data["structure"]["dimensions"]["observation"]
        except KeyError as exc:
            raise EurostatAPIError(
                f"Missing dimensions in JSON: {exc}"
            ) from exc

        dim_names = [d["id"] for d in dims]
        dim_categories = {d["id"]: d["values"] for d in dims}

        # ---- Extract observations ----
        try:
            observations = json_data["dataSets"][0]["observations"]
        except (KeyError, IndexError) as exc:
            raise EurostatAPIError(
                f"Missing observations in JSON: {exc}"
            ) from exc

        rows = []
        for key, value in observations.items():
            # Key is like "0:1:3:5" → indices into dimension value arrays
            indices = list(map(int, key.split(":")))
            obs_value = value[0] if value else None

            row: Dict[str, Any] = {"value": obs_value}

            for dim_name, idx in zip(dim_names, indices):
                dim_values = dim_categories.get(dim_name, [])
                try:
                    entry = dim_values[idx]
                except IndexError as exc:
                    raise EurostatAPIError(
                        f"Index {idx} out of range for dimension {dim_name}"
                    ) from exc

                # Prefer 'name', fall back to 'id'
                label = entry.get("name") or entry.get("id")
                row[dim_name] = label

            rows.append(row)

        df = pd.DataFrame(rows)

        # Standardize time column
        if "TIME_PERIOD" in df.columns:
            df.rename(columns={"TIME_PERIOD": "period"}, inplace=True)

        return df

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------
    def describe_dimensions(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Flatten SDMX dimensions into a table:

            dimension | code | label
        """
        try:
            dims = json_data["structure"]["dimensions"]["observation"]
        except KeyError as exc:
            raise EurostatAPIError(
                f"Missing dimensions in JSON: {exc}"
            ) from exc

        rows = []
        for dim in dims:
            dim_id = dim["id"]
            for val in dim.get("values", []):
                rows.append(
                    {
                        "dimension": dim_id,
                        "code": val.get("id"),
                        "label": val.get("name"),
                    }
                )

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Internal: caching
    # ------------------------------------------------------------------
    def _cache_path_for_url(self, url: str) -> Path:
        assert self.cache_dir is not None
        key = hashlib.sha1(url.encode("utf-8")).hexdigest()
        return Path(self.cache_dir) / f"{key}.json"

    def _read_from_cache(self, url: str) -> Optional[Dict[str, Any]]:
        if self.cache_dir is None:
            return None

        path = self._cache_path_for_url(url)
        if not path.exists():
            return None

        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Failed to read cache file %s: %s", path, exc)
            return None

    def _write_to_cache(self, url: str, data: Dict[str, Any]) -> None:
        if self.cache_dir is None:
            return

        path = self._cache_path_for_url(url)
        try:
            with path.open("w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Failed to write cache file %s: %s", path, exc)


# ----------------------------------------------------------------------
# Quick manual test / demo
# ----------------------------------------------------------------------
if __name__ == "__main__":
    api = EurostatAPI(cache_dir=".cache/eurostat")

    dataset = "STS_RT_M"
    filters = {
        "geo": "DE",
        "s_adj": "NSA",
        "unit": "I15",
        "sts_activity": "G47",
    }

    df = api.get_dataset(dataset, filters)
    print(df.head())
    print("Rows:", len(df))
