
import csv
import os
import tempfile
from datetime import datetime, timedelta, timezone

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)


# Configuration
PROJECT_ID = "worldbank-data-pipeline-514619"
BUCKET = "worldbank-data-pipeline-514619-worldbank-raw"
DATASET = "raw_worldbank"

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]

INDICATORS = [
    "NY.GDP.MKTP.CD",
    "FP.CPI.TOTL.ZG",
    "SP.POP.TOTL",
    "SL.UEM.TOTL.ZS",
    "SP.DYN.LE00.IN",
]

BASE_URL = "https://api.worldbank.org/v2"


def extract_worldbank_data(countries, indicators, load_date):
    rows = []

    for country in countries:
        for indicator in indicators:
            page = 1

            while True:
                url = (
                    f"{BASE_URL}/country/{country}"
                    f"/indicator/{indicator}"
                )

                response = requests.get(
                    url,
                    params={
                        "format": "json",
                        "page": page,
                        "per_page": 1000,
                    },
                    timeout=30,
                )

                response.raise_for_status()
                data = response.json()

                # Detect errors returned with HTTP 200
                if (
                    not isinstance(data, list)
                    or len(data) != 2
                    or not isinstance(data[0], dict)
                    or "message" in data[0]
                    or not isinstance(data[1], list)
                ):
                    raise ValueError(
                        f"World Bank API error "
                        f"({country}, {indicator}): {data}"
                    )

                metadata, observations = data

                for obs in observations:
                    if obs.get("value") is None:
                        continue

                    rows.append({
                        "country_code": obs["countryiso3code"],
                        "country_name": obs["country"]["value"],
                        "indicator_code": obs["indicator"]["id"],
                        "indicator_label": obs["indicator"]["value"],
                        "year": int(obs["date"]),
                        "value": float(obs["value"]),
                        "load_date": load_date,
                    })

                if page >= int(metadata["pages"]):
                    break

                page += 1

    if not rows:
        raise ValueError("No World Bank observations extracted")

    return rows


def extract_and_upload_to_gcs():
    context = get_current_context()
    execution_date = context["ds"]

    rows = extract_worldbank_data(
        COUNTRIES,
        INDICATORS,
        execution_date,
    )

    columns = [
        "country_code",
        "country_name",
        "indicator_code",
        "indicator_label",
        "year",
        "value",
        "load_date",
    ]

    object_name = (
        f"worldbank/{execution_date}/indicators.csv"
    )

    temporary_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            newline="",
            encoding="utf-8",
            delete=False,
        ) as file:
            temporary_path = file.name

            writer = csv.DictWriter(
                file,
                fieldnames=columns,
            )
            writer.writeheader()
            writer.writerows(rows)

        gcs_hook = GCSHook()

        gcs_hook.upload(
            bucket_name=BUCKET,
            object_name=object_name,
            filename=temporary_path,
            mime_type="text/csv",
        )

        print(
            f"Uploaded {len(rows)} rows to "
            f"gs://{BUCKET}/{object_name}"
        )

    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.remove(temporary_path)


with DAG(
    dag_id="worldbank_gdp_ingestion",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="@weekly",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["worldbank", "gcp", "ingestion"],
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET,
        source_objects=[
            "worldbank/{{ ds }}/indicators.csv"
        ],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET}.indicators"
        ),
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=False,
        schema_fields=[
            {"name": "country_code", "type": "STRING"},
            {"name": "country_name", "type": "STRING"},
            {"name": "indicator_code", "type": "STRING"},
            {"name": "indicator_label", "type": "STRING"},
            {"name": "year", "type": "INT64"},
            {"name": "value", "type": "FLOAT64"},
            {"name": "load_date", "type": "DATE"},
        ],
        location="europe-west9",
        time_partitioning={
            "type": "DAY",
            "field": "load_date",
        },
        cluster_fields=["country_code"],
    )

    check_data_quality = BigQueryCheckOperator(
        task_id="check_data_quality",
        sql=f"""
            SELECT COUNT(DISTINCT country_code) = {len(COUNTRIES)}
            FROM `{PROJECT_ID}.{DATASET}.indicators`
            WHERE country_code IN (
                'MAR', 'TUN', 'DZA', 'EGY', 'FRA'
            )
        """,
        use_legacy_sql=False,
        location="europe-west9",
    )

    extract_and_upload >> load_to_bigquery >> check_data_quality
