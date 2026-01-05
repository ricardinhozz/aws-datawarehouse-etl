from datetime import date
import uuid
import boto3
from io import StringIO


def generate_s3_key(table_name):
    today = date.today().isoformat()
    return f"staging/{table_name}/dt={today}/{uuid.uuid4()}.csv"


def upload_df(df, bucket, key):
    buffer = StringIO()
    df.to_csv(buffer, index=False)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )


def delete_object(bucket, key):
    boto3.client("s3").delete_object(
        Bucket=bucket,
        Key=key
    )
