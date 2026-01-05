WEATHER_HOURLY_DDL = """
CREATE TABLE IF NOT EXISTS weather_hourly (
    city VARCHAR(100),
    lat FLOAT,
    lon FLOAT,
    country_code VARCHAR(10),
    datetime TIMESTAMP,
    temp FLOAT,
    description VARCHAR(255),
    code INTEGER,
    timezone VARCHAR(50),
    source VARCHAR(20)
);
"""

WEATHER_HOURLY_STAGING_DDL = """
CREATE TABLE IF NOT EXISTS weather_hourly_staging
(LIKE weather_hourly);
"""
