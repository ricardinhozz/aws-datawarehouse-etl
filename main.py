import os
from etl.extract.api1_extractor import extract_weatherbit_api
from etl.extract.api2_extractor import extract_api_open_meteo
from etl.transform.data_transformer import (
    transform_data_from_api_1,
    transform_data_from_api_2,
    merge_and_deduplicate
)
from etl.load.redshift_loader import load_dataframe

api_data_1 = extract_weatherbit_api()
api_data_2 = extract_api_open_meteo()

df1 = transform_data_from_api_1(api_data_1)
df2 = transform_data_from_api_2(api_data_2)

final_df = merge_and_deduplicate(df1, df2)

load_dataframe(
    df=final_df,
    table="weather_hourly",
    staging_table="weather_hourly_staging",
    s3_bucket=os.environ["S3_BUCKET"],
    iam_role=os.environ["REDSHIFT_IAM_ROLE"],
    database=os.getenv("REDSHIFT_DATABASE", "dev"),
    workgroup=os.getenv("REDSHIFT_WORKGROUP", "default-workgroup")
)
