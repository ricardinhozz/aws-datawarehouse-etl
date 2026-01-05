import pandas as pd 

def transform_data_from_api_1(api_data_1):
    hourly_data = api_data_1.get("data", [])

    if not hourly_data:
        return pd.DataFrame()

    df = pd.DataFrame(hourly_data)

    columns_to_keep = [c for c in ["datetime", "app_temp", "temp", "weather"] if c in df.columns]
    df = df[columns_to_keep]

    if "weather" in df.columns:
        weather_df = pd.json_normalize(df["weather"])
        df = df.drop(columns=["weather"]).join(weather_df)

    top_level_cols = {
        "city": api_data_1.get("city_name"),
        "lat": api_data_1.get("lat"),
        "lon": api_data_1.get("lon"),
        "country_code": api_data_1.get("country_code"),
        "timezone": api_data_1.get("timezone")
    }

    for c, v in top_level_cols.items():
        df[c] = v

    df["source"] = "api_1"

    cols_order = [
        "city", "lat", "lon", "country_code", "datetime",
        "temp", "description", "code", "timezone", "source"
    ]

    return df[[c for c in cols_order if c in df.columns]]


    

def transform_data_from_api_2(api_data_2):
    hourly = api_data_2.get("hourly", {})
    times = hourly.get("time", [])

    if not times:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)

    df = df.rename(columns={
        "temperature_2m": "temp",
        "cloud_cover": "code",
        "time": "datetime"
    })

    df["lat"] = api_data_2.get("latitude")
    df["lon"] = api_data_2.get("longitude")
    df["city"] = api_data_2.get("city_name", "São Paulo")
    df["country_code"] = api_data_2.get("country_code")
    df["timezone"] = api_data_2.get("timezone")
    df["description"] = None
    df["source"] = "api_2"

    cols_order = [
        "city", "lat", "lon", "country_code", "datetime",
        "temp", "description", "code", "timezone", "source"
    ]

    return df[[c for c in cols_order if c in df.columns]]




def merge_and_deduplicate(df1, df2):
    all_cols = sorted(set(df1.columns).union(df2.columns))

    df1 = df1.reindex(columns=all_cols)
    df2 = df2.reindex(columns=all_cols)

    df = pd.concat([df1, df2], ignore_index=True)

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    #keep latest source (api_2 > api_1 if same datetime)
    df = (
        df.sort_values(["city", "datetime", "source"])
          .drop_duplicates(subset=["city", "datetime"], keep="last")
          .reset_index(drop=True)
    )

    return df

