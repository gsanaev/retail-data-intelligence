import requests
import pandas as pd
from typing import Dict, Any


class EurostatAPI:
    """
    Eurostat SDMX JSON API client (2025 version).
    
    Uses the official SDMX endpoint:
        https://ec.europa.eu/eurostat/api/discover/sdmx

    Example:
        api = EurostatAPI()
        df = api.get_dataset(
            "STS_RT_M",
            {
                "geo": "DE",
                "s_adj": "NSA",
                "unit": "I15",
                "sts_activity": "G47"
            }
        )
    """

    def __init__(
        self,
        base_url: str = "https://ec.europa.eu/eurostat/api/discover/sdmx",
        timeout: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # -----------------------------------------------------
    # Public API
    # -----------------------------------------------------
    def get_dataset(self, dataset_code: str, filters: Dict[str, str]) -> pd.DataFrame:
        url = self.build_url(dataset_code, filters)
        raw_json = self.fetch_json(url)
        df = self.to_dataframe(raw_json)
        return df

    # -----------------------------------------------------
    # Build URL
    # -----------------------------------------------------
    def build_url(self, dataset_code: str, filters: Dict[str, str]) -> str:
        filter_string = "&".join([f"{k}={v}" for k, v in filters.items()])
        return f"{self.base_url}?table={dataset_code}&format=JSON&{filter_string}"

    # -----------------------------------------------------
    # Fetch JSON
    # -----------------------------------------------------
    def fetch_json(self, url: str) -> Dict[str, Any]:
        """Perform a GET request and return parsed JSON."""

        # Pretend to be a real browser (Eurostat blocks bots)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://ec.europa.eu/eurostat/",
        }

        response = requests.get(url, headers=headers, timeout=self.timeout)

        if response.status_code != 200:
            raise ValueError(
                f"Eurostat API returned status {response.status_code}: {response.text[:200]}"
            )

        data = response.json()

        if "structure" not in data or "dataSets" not in data:
            raise ValueError(
                f"Unexpected response format: keys={list(data.keys())}"
            )

        return data


    # -----------------------------------------------------
    # SDMX JSON → tidy DataFrame
    # -----------------------------------------------------
    def to_dataframe(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts SDMX JSON into a tidy DataFrame.
        Extracts observations, dimensions, and attributes.
        """

        # --- Extract dimensions ---
        dims = json_data["structure"]["dimensions"]["observation"]
        dim_names = [d["id"] for d in dims]
        dim_categories = {
            d["id"]: d["values"] for d in dims
        }

        # --- Extract data ---
        observations = json_data["dataSets"][0]["observations"]

        rows = []
        for key, value in observations.items():
            indices = list(map(int, key.split(":")))
            obs_value = value[0] if value else None

            # Build row with dimension labels
            row = {"value": obs_value}
            for dim_name, idx in zip(dim_names, indices):
                row[dim_name] = dim_categories[dim_name][idx]["name"]

            rows.append(row)

        df = pd.DataFrame(rows)

        # Rename time dimension if present
        if "TIME_PERIOD" in df.columns:
            df.rename(columns={"TIME_PERIOD": "period"}, inplace=True)

        return df
