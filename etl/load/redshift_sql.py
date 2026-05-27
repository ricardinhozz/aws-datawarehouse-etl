import re

_SAFE_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,127}$')
_SAFE_IAM_ROLE = re.compile(r'^arn:aws:iam::\d{12}:role/[\w+=,.@/-]+$')
_SAFE_S3_PATH = re.compile(r'^s3://[a-z0-9][a-z0-9\-\.]{1,61}[a-z0-9]/.+$')


def _validate_identifier(name):
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")


def _validate_iam_role(arn):
    if not _SAFE_IAM_ROLE.match(arn):
        raise ValueError(f"Unsafe IAM role ARN: {arn!r}")


def _validate_s3_path(path):
    if not _SAFE_S3_PATH.match(path):
        raise ValueError(f"Unsafe S3 path: {path!r}")


def copy_sql(table, s3_path, iam_role):
    _validate_identifier(table)
    _validate_s3_path(s3_path)
    _validate_iam_role(iam_role)
    return f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto';
    """


def merge_sql(final_table, staging_table):
    _validate_identifier(final_table)
    _validate_identifier(staging_table)
    return f"""
    BEGIN;

    DELETE FROM {final_table}
    USING {staging_table}
    WHERE {final_table}.city = {staging_table}.city
      AND {final_table}.datetime = {staging_table}.datetime;

    INSERT INTO {final_table}
    SELECT * FROM {staging_table};

    TRUNCATE TABLE {staging_table};

    END;
    """
