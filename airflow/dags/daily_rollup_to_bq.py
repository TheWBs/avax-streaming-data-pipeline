from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow.dataset as ds
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.api_core import exceptions as gax_exceptions
from google.cloud import bigquery

BQ_PROJECT = os.environ.get("BQ_PROJECT", "crypto-project-489022")
BQ_DATASET = os.environ.get("BQ_DATASET", "crypto")
BQ_TABLE = os.environ.get("BQ_TABLE", "avax_daily")
MINUTE_PARQUET_PATH = os.environ.get("MINUTE_PARQUET_PATH", "/data/minute")


def _yesterday_utc_date() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


def _resolve_day(target_day: str | None, data_interval_end: datetime | None) -> str:
    if target_day:
        return target_day
    if data_interval_end:
        return (data_interval_end - timedelta(days=1)).date().isoformat()
    return _yesterday_utc_date()


def rollup_and_load(target_day: str | None = None, data_interval_end: datetime | None = None, **_):
    day = _resolve_day(target_day=target_day, data_interval_end=data_interval_end)

    if not os.path.exists(MINUTE_PARQUET_PATH):
        print(f"Minute parquet path does not exist yet: {MINUTE_PARQUET_PATH}")
        return

    dataset = ds.dataset(MINUTE_PARQUET_PATH, format="parquet")
    # Spark writes minute_ts as timezone-naive timestamp in parquet.
    start = pd.Timestamp(day)
    end = start + pd.Timedelta(days=1)

    table = dataset.to_table(
        filter=(ds.field("minute_ts") >= start.to_pydatetime()) & (ds.field("minute_ts") < end.to_pydatetime())
    )

    if table.num_rows == 0:
        print(f"No minute rows found for day={day}. Nothing to load.")
        return

    df = table.to_pandas()
    df["minute_ts"] = pd.to_datetime(df["minute_ts"], utc=True)
    df = df.sort_values(["symbol", "minute_ts"])

    out = (
        df.groupby("symbol", as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            trade_count=("trade_count", "sum"),
            base_volume=("base_volume", "sum"),
            taker_buy_base_volume=("taker_buy_base_volume", "sum"),
            taker_sell_base_volume=("taker_sell_base_volume", "sum"),
        )
    )
    out.insert(1, "day", pd.to_datetime(day).date())
    out["trade_count"] = out["trade_count"].astype("int64")

    out["taker_buy_ratio"] = out["taker_buy_base_volume"] / (out["base_volume"] + 1e-9)
    out["created_at"] = datetime.now(timezone.utc)

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Keep daily loads idempotent for retries/backfills when DML is available.
    try:
        delete_job = client.query(
            f"DELETE FROM `{table_id}` WHERE day = @day",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
            ),
        )
        delete_job.result()
    except gax_exceptions.Forbidden as e:
        if "billingNotEnabled" in str(e):
            print("Billing is not enabled for DML in this project; skipping DELETE and using APPEND load.")
        else:
            raise

    job = client.load_table_from_dataframe(out, table_id, job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    ))
    job.result()
    print(f"Loaded {len(out)} rows into {table_id} for day={day}.")


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="avax_daily_rollup_to_bq",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=False,
    tags=["avax", "bigquery"],
) as dag:
    PythonOperator(
        task_id="rollup_and_load_to_bigquery",
        python_callable=rollup_and_load,
    )
