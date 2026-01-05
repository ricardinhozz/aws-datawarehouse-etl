def copy_sql(table, s3_path, iam_role):
    return f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto';
    """


def merge_sql(final_table, staging_table):
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
