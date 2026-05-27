import time
import boto3
from etl.load.s3 import generate_s3_key, upload_df, delete_object
from etl.load.redshift_sql import copy_sql, merge_sql


def execute_sql(sql, database, workgroup):
    client = boto3.client("redshift-data")

    resp = client.execute_statement(
        Database=database,
        WorkgroupName=workgroup,
        Sql=sql
    )

    while True:
        status = client.describe_statement(Id=resp["Id"])
        if status["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            return status
        time.sleep(1)


def load_dataframe(
    df,
    table,
    staging_table,
    s3_bucket,
    iam_role,
    database,
    workgroup
):
    s3_key = generate_s3_key(table)
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    upload_df(df, s3_bucket, s3_key)

    try:
        result = execute_sql(
            copy_sql(staging_table, s3_path, iam_role),
            database,
            workgroup
        )
        if result["Status"] != "FINISHED":
            raise RuntimeError(f"COPY failed with status: {result['Status']}")

        result = execute_sql(
            merge_sql(table, staging_table),
            database,
            workgroup
        )
        if result["Status"] != "FINISHED":
            raise RuntimeError(f"MERGE failed with status: {result['Status']}")
    finally:
        delete_object(s3_bucket, s3_key)
