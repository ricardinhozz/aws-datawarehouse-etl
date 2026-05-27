import re
import uuid
import time
import boto3
from io import StringIO

_SAFE_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,127}$')


def _validate_identifier(name):
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")


def upload_df_to_s3(df, bucket, key):
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ServerSideEncryption="AES256"
    )


def wait_for_statement(redshift_data, statement_id):
    """
    Polls Redshift Data API until query finishes
    """
    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status in ("FINISHED", "FAILED", "ABORTED"):
            return desc

        time.sleep(1)


def execute_redshift_sql(
    sql,
    database,
    workgroup_name=None,
    cluster_identifier=None
):
    """
    Execute SQL using Redshift Data API
    """
    client = boto3.client("redshift-data")

    params = {
        "Database": database,
        "Sql": sql
    }

    if workgroup_name:
        params["WorkgroupName"] = workgroup_name
    elif cluster_identifier:
        params["ClusterIdentifier"] = cluster_identifier
    else:
        raise ValueError("Either workgroup_name or cluster_identifier must be provided")

    response = client.execute_statement(**params)

    return wait_for_statement(client, response["Id"])


def load_dataframe_to_redshift(
    df,
    table_name,
    s3_bucket,
    iam_role_arn,
    database,
    workgroup_name=None,
    cluster_identifier=None
):
    _validate_identifier(table_name)

    s3_key = f"staging/{table_name}/{uuid.uuid4()}.csv"
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    upload_df_to_s3(df, s3_bucket, s3_key)

    copy_sql = f"""
        COPY {table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        EMPTYASNULL
        BLANKSASNULL;
    """

    try:
        result = execute_redshift_sql(
            sql=copy_sql,
            database=database,
            workgroup_name=workgroup_name,
            cluster_identifier=cluster_identifier
        )
        if result["Status"] != "FINISHED":
            raise RuntimeError(f"COPY failed with status: {result['Status']}")
    finally:
        boto3.client("s3").delete_object(Bucket=s3_bucket, Key=s3_key)