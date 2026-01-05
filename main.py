from etl.transform.data_transformer import (
    transform_data_from_api_1,
    transform_data_from_api_2,
    merge_and_deduplicate
)
from etl.load.redshift_loader import load_dataframe

df1 = transform_data_from_api_1(api_data_1)
df2 = transform_data_from_api_2(api_data_2)

final_df = merge_and_deduplicate(df1, df2)

load_dataframe(
    df=final_df,
    table="weather_hourly",
    staging_table="weather_hourly_staging",
    s3_bucket="api-warehouse-datac",
    iam_role="arn:aws:iam::713758830452:role/service-role/AmazonRedshift-CommandsAccessRole-20251208T104722",
    database="dev",
    workgroup="default-workgroup"
)
