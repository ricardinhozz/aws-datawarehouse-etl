import pandas as pd 

def transform_data_from_api_1(api_data_1):
        
    hourly_data = api_data_1.get("data", [])

    if not hourly_data:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(hourly_data)
        
        columns_to_keep = [col for col in ["datetime", "app_temp", "temp", "weather"] if col in df.columns]
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
        
        cols_order = ["city", "lat", "lon", "country_code", "datetime", "temp", 
                    "description", "code", "timezone"]
        df = df[[c for c in cols_order if c in df.columns]]
    return df

    

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
    
    #add top-level info due to different api data structure

    df["lat"] = api_data_2.get("latitude")
    df["lon"] = api_data_2.get("longitude")
    df["city"] = api_data_2.get("city_name", None)  
    df["country_code"] = api_data_2.get("country_code", None)
    df["timezone"] = api_data_2.get("timezone")
    
    if "description" not in df.columns:
        df["description"] = None
    
    cols_order = ["city", "lat", "lon", "country_code", "datetime", "temp", 
                  "description",  "timezone"]
    df = df[[c for c in cols_order if c in df.columns]]
    df['city'] = 'São Paulo'
    return df
