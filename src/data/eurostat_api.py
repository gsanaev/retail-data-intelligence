import requests
import pandas as pd
from typing import Dict, Any


class EurostatAPI:
    """
    Simple synchronous Eurostat API client for retrieving datasets
    and converting them into pandas DataFrames.

    Usage:
    -------
    api = EurostatAPI()
    df = api.get_dataset(
        dataset_code="STS_RT_M",
        filters={"geo": "DE", "s_adj": "SA", "unit": "I15"}
    )
    """

    def __init__(
        self,
        base_url: str = "https://ec.europa.eu/eurostat/api/discover/tables",
        timeout: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # -----------------------------------------------------
    # Public API
    # -----------------------------------------------------
    def get_dataset(self, dataset_code: str, filters: Dict[str, str]) -> pd.DataFrame:
        """
        High-level method that:
        1. Builds URL
        2. Retrieves raw JSON
        3. Converts to DataFrame
        """
        url = self.build_url(dataset_code, filters)
        raw = self.fetch_json(url)
        df = self.to_dataframe(raw)
        return df

    # -----------------------------------------------------
    # URL builder
    # -----------------------------------------------------
    def build_url(self, dataset_code: str, filters: Dict[str, str]) -> str:
        """
        Build a Eurostat API URL using dataset code and query filters.
        Example:
            dataset_code="STS_RT_M"
            filters={"geo": "DE", "unit": "I15", "s_adj": "SA"}
        """
        filter_string = "&".join([f"{k}={v}" for k, v in filters.items()])
        url = f"{self.base_url}/{dataset_code}?{filter_string}"
        return url

    # -----------------------------------------------------
    # Fetch JSON
    # -----------------------------------------------------
    def fetch_json(self, url: str) -> Dict[str, Any]:
        """
        Perform a GET request and return parsed JSON.
        Raises a clear error if request fails.
        """
        response = requests.get(url, timeout=self.timeout)

        if response.status_code != 200:
            raise ValueError(
                f"Eurostat API returned status {response.status_code}: {response.text}"
            )
        return response.json()

    # -----------------------------------------------------
    # JSON → DataFrame
    # -----------------------------------------------------
    def to_dataframe(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Convert Eurostat JSON structure into a clean pandas DataFrame.

        Eurostat JSON structure:
        {
          "value": {...},
          "dimension": {...},
          "id": [...],
          "size": [...]
        }

        Output DataFrame:
        period | geo | value | other_dims...
        """
        # Extract dimensions
        dims = json_data.get("dimension", {})
        dimension_ids = json_data.get("id", [])

        # Build index mapping
        dimension_categories = {
            dim: dims[dim]["category"]["index"]
            for dim in dimension_ids
        }

        # Build reverse lookup: 0 → "DE", 1 → "FR", etc.
        reverse_maps = {
            dim: {v: k for k, v in cat.items()}
            for dim, cat in dimension_categories.items()
        }

        # Extract values
        values = json_data.get("value", {})

        # Prepare rows
        rows = []
        size = json_data.get("size", [])

        # Cartesian index: (i, j, k, ...)
        import itertools

        for idx_tuple in itertools.product(*[range(s) for s in size]):
            # Find flat index
            flat_index = 0
            multiplier = 1

            for dim_size, index in zip(reversed(size), reversed(idx_tuple)):
                flat_index += index * multiplier
                multiplier *= dim_size

            value = values.get(str(flat_index), None)

            row = {"value": value}

            # Map each dimension index → label
            for dim_name, index_val in zip(dimension_ids, idx_tuple):
                label = reverse_maps[dim_name].get(index_val)
                row[dim_name] = label

            rows.append(row)

        df = pd.DataFrame(rows)

        # Rename time column if present
        if "time" in df.columns:
            df.rename(columns={"time": "period"}, inplace=True)

        return df
