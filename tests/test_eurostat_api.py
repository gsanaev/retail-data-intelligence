from src.data.eurostat_api import EurostatAPI


def run_test():
    api = EurostatAPI()

    dataset = "STS_RT_M"

    filters = {
        "geo": "DE",
        "s_adj": "NSA",
        "unit": "I15",
        "sts_activity": "G47"   # REQUIRED dimension
    }

    print("Building URL…")
    url = api.build_url(dataset, filters)
    print("URL:", url)

    print("Fetching JSON…")
    json_data = api.fetch_json(url)
    print("JSON keys:", list(json_data.keys()))

    print("Converting to DataFrame…")
    df = api.to_dataframe(json_data)

    print("DataFrame shape:", df.shape)
    print(df.head())

    print("\n✔ Eurostat API test completed successfully!")


if __name__ == "__main__":
    run_test()
