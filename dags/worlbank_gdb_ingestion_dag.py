"""Pipeline World Bank : API JSON, CSV sur GCS, puis BigQuery."""

import csv
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT = "worldbank-data-pipeline-514619"
BUCKET = "worldbank-data-pipeline-514619-worldbank-raw"
COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = [
    "NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG", "SP.POP.TOTL",
    "SL.UEM.TOTL.ZS", "SP.DYN.LE00.IN",
]
COLUMNS = [
    "country_code", "country_name", "indicator_code", "indicator_label",
    "year", "value", "load_date",
]


def extract_worldbank_data(ds):
    countries = ";".join(COUNTRIES)
    indicators = ";".join(INDICATORS)
    url = f"https://api.worldbank.org/v2/country/{countries}/indicator/{indicators}"

    response = requests.get(
        url,
        params={"source": 2, "format": "json", "per_page": 20000},
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()

    # Détecter aussi les erreurs renvoyées avec HTTP 200.
    if (
        not isinstance(data, list) or len(data) != 2
        or not isinstance(data[0], dict) or "message" in data[0]
        or not isinstance(data[1], list)
    ):
        raise ValueError(f"Erreur API World Bank : {str(data)[:300]}")

    metadata, observations = data
    if int(metadata["pages"]) != 1 or len(observations) != int(metadata["total"]):
        raise ValueError("Réponse incomplète : toutes les données doivent tenir dans cet appel")

    # Sélectionner les sept colonnes nécessaires, sans les valeurs nulles.
    rows = []
    for item in observations:
        if item["value"] is not None:
            rows.append([
                item["countryiso3code"], item["country"]["value"],
                item["indicator"]["id"], item["indicator"]["value"],
                int(item["date"]), float(item["value"]), ds,
            ])
    if not rows:
        raise ValueError("Aucune donnée extraite")
    return rows


def extract_and_upload(ds, **_):
    rows = extract_worldbank_data(ds)

    # Le dossier temporaire est supprimé automatiquement après l'upload.
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "indicators.csv"
        with path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(COLUMNS)
            writer.writerows(rows)

        GCSHook().upload(
            bucket_name=BUCKET,
            object_name=f"worldbank/{ds}/indicators.csv",
            filename=str(path),
            mime_type="text/csv",
        )

    print(f"{len(rows)} lignes envoyées sur GCS")


with DAG(
    dag_id="worldbank_gdp_ingestion",
    schedule="@weekly",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "data-eng-bootcamp", "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    tags=["worldbank", "gcp", "ingestion"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload,
    )

    load_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET,
        source_objects=["worldbank/{{ ds }}/indicators.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw_worldbank.indicators",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "country_code", "type": "STRING"},
            {"name": "country_name", "type": "STRING"},
            {"name": "indicator_code", "type": "STRING"},
            {"name": "indicator_label", "type": "STRING"},
            {"name": "year", "type": "INT64"},
            {"name": "value", "type": "FLOAT64"},
            {"name": "load_date", "type": "DATE"},
        ],
        time_partitioning={"type": "DAY", "field": "load_date"},
        cluster_fields=["country_code"],
        location="europe-west9",
    )

    check_task = BigQueryCheckOperator(
        task_id="check_data_quality",
        sql=f"""
            SELECT COUNT(DISTINCT country_code) = {len(COUNTRIES)}
            FROM `{PROJECT}.raw_worldbank.indicators`
            WHERE country_code IN ('MAR', 'TUN', 'DZA', 'EGY', 'FRA')
        """,
        use_legacy_sql=False,
        location="europe-west9",
    )

    extract_task >> load_task >> check_task
