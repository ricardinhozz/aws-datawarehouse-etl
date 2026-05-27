# AWS Data Warehouse — Weather ETL Pipeline

An ETL pipeline that ingests hourly weather data from two public APIs, merges and deduplicates the records, and loads them into Amazon Redshift Serverless via an S3 staging layer.

---

## Architecture

```
┌─────────────────────┐    ┌──────────────────────┐
│  Weatherbit API      │    │  Open-Meteo API       │
│  (historical hourly) │    │  (forecast/hourly)    │
└────────┬────────────┘    └─────────┬────────────┘
         │                           │
         ▼                           ▼
    api1_extractor              api2_extractor
         │                           │
         ▼                           ▼
  transform_data_from_api_1   transform_data_from_api_2
         │                           │
         └──────────┬────────────────┘
                    ▼
          merge_and_deduplicate
                    │
                    ▼
           Upload CSV to S3
          (staging/<table>/dt=YYYY-MM-DD/<uuid>.csv)
                    │
                    ▼
         Redshift COPY into staging table
                    │
                    ▼
         Merge (DELETE + INSERT) into final table
                    │
                    ▼
         Delete S3 staging file
```

**AWS services used:**
- **S3** — temporary staging layer for CSV files
- **Redshift Serverless** — analytical data warehouse
- **Redshift Data API** — SQL execution via `boto3` (no direct TCP connection needed)
- **IAM** — role-based access for Redshift to read from S3

---

## Project Structure

```
aws-datawarehouse/
├── aws-datawarehouse-etl/
│   ├── aws-datawarehouse-etl/
│   │   ├── etl/
│   │   │   ├── extract/
│   │   │   │   ├── api1_extractor.py     #Weatherbit historical hourly API
│   │   │   │   └── api2_extractor.py     #Open-Meteo forecast API
│   │   │   ├── transform/
│   │   │   │   └── data_transformer.py   #Normalize + merge both DataFrames
│   │   │   └── load/
│   │   │       ├── ddl.py                #CREATE TABLE statements
│   │   │       ├── s3.py                 #S3 upload / delete helpers
│   │   │       ├── redshift_sql.py       #COPY and MERGE SQL generators
│   │   │       ├── redshift_loader.py    #Orchestrates S3 staging → Redshift
│   │   │       └── data_loader.py        #Alternative loader (Redshift Data API)
│   │   ├── main.py                       #Pipeline entry point
│   │   └── requirements.txt
│   └── keys.py                           #Local credentials (gitignored)
└── dummy_data/                           #Sample CSVs for a banking star schema
    ├── Dim*.csv                          #Dimension tables
    └── Fact*.csv                         #Fact tables
```

---

# Data Model

The pipeline writes to a single Redshift table:

```sql
CREATE TABLE IF NOT EXISTS weather_hourly (
    city         VARCHAR(100),
    lat          FLOAT,
    lon          FLOAT,
    country_code VARCHAR(10),
    datetime     TIMESTAMP,
    temp         FLOAT,
    description  VARCHAR(255),
    code         INTEGER,
    timezone     VARCHAR(50),
    source       VARCHAR(20)
);
```

A mirror `weather_hourly_staging` table (same schema) is used as the intermediary for each load run and is cleared after each merge.

---

## Data Sources

| API | Provider | Data | Location |
|-----|----------|------|----------|
| Weatherbit | [weatherbit.io](https://www.weatherbit.io/) | Historical hourly weather (temp, description, weather code) | São Paulo, BR |
| Open-Meteo | [open-meteo.com](https://open-meteo.com/) | Hourly forecast (temp, humidity, rain, cloud cover) | São Paulo, BR (`-23.5475, -46.6361`) |

Each run fetches data for **yesterday → today**. When both APIs return records for the same city + datetime, Open-Meteo (`api_2`) takes precedence during deduplication.

---

## Prerequisites

- Python 3.10+
- AWS credentials configured (`~/.aws/credentials` or environment variables)
- An existing **Redshift Serverless** workgroup and database
- An IAM role that allows Redshift to read from the S3 staging bucket
- The `weather_hourly` and `weather_hourly_staging` tables created (DDL in [ddl.py](aws-datawarehouse-etl/aws-datawarehouse-etl/etl/load/ddl.py))
- A `WEATHER_API_KEY` environment variable for the Weatherbit API

---

## Setup

```bash
cd aws-datawarehouse-etl/aws-datawarehouse-etl
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `keys.py` file (gitignored) or export the required environment variables:

```python
# keys.py
import os
os.environ["WEATHER_API_KEY"] = "<your-weatherbit-api-key>"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
```

---

## Running the Pipeline

```bash
python main.py
```

The pipeline will:
1. Extract hourly weather from Weatherbit and Open-Meteo
2. Normalize each dataset into a common schema
3. Merge and deduplicate by `(city, datetime)`
4. Stage the result as a CSV in S3
5. `COPY` into the Redshift staging table
6. Merge (upsert) records into `weather_hourly`
7. Clean up the S3 staging file

---

## Redshift Configuration

Default values used in `main.py`:

| Parameter | Value |
|-----------|-------|
| Workgroup | `default-workgroup` |
| Database | `dev` |
| S3 Bucket | `api-warehouse-datac` |
| Region | `us-east-1` |

---

## Dummy Data

The `dummy_data/` directory contains pipe-delimited sample CSVs modelling a **banking star schema**, useful for testing Redshift ingestion independently of the weather pipeline:

**Dimensions:** `DimAccount`, `DimChannel`, `DimCurrency`, `DimCustomers`, `DimDate`, `DimInvestment`, `DimLoan`, `DimLocation`, `DimTransactionType`

**Facts:** `FactTransactions`, `FactDailyBalances`, `FactInvestments`, `FactLoanPayments`, `FactCustomerInteraction`

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `pandas` | DataFrame operations and CSV serialization |
| `numpy` | Numerical support |
| `requests` | HTTP calls to both weather APIs |
| `boto3` | AWS SDK — S3 and Redshift Data API |
