# AVAX Streaming Data Pipeline

Real-time crypto market data pipeline built for a Data Engineering portfolio.
The system ingests live AVAXUSDT aggTrades from Binance, transforms them into minute-level aggregates, and publishes daily analytics to BigQuery.

## Why this project matters
- Demonstrates end-to-end streaming architecture, not just batch ETL.
- Shows practical orchestration with Airflow and production-like containerized deployment.
- Includes data modeling for analytics-ready OHLC/volume metrics.
- Uses cloud warehouse integration (BigQuery) for downstream BI and reporting.

## Architecture

```text
Binance WebSocket (aggTrade)
  -> Producer (Python)
  -> Kafka topic (binance_avaxusdt_aggtrades)
  -> Spark Structured Streaming
  -> Minute parquet (/data/minute)
  -> Airflow daily rollup DAG
  -> BigQuery table: crypto.avax_daily
```

Architecture image:
![Architecture Diagram](docs/images/architecture-diagram.svg)

## Tech stack
- Python 3.11
- Apache Kafka + ZooKeeper
- Apache Spark Structured Streaming
- Apache Airflow 2.9
- Google BigQuery
- Docker Compose

## Data outputs
### Minute-level parquet (`/data/minute`)
- `symbol`
- `minute_ts`
- `open`, `high`, `low`, `close`
- `trade_count`
- `base_volume`
- `taker_buy_base_volume`, `taker_sell_base_volume`
- `taker_buy_ratio`

### BigQuery daily table (`crypto.avax_daily`)
- `symbol`
- `day`
- `open`, `high`, `low`, `close`
- `trade_count`
- `base_volume`
- `taker_buy_base_volume`, `taker_sell_base_volume`
- `taker_buy_ratio`
- `created_at`

## Repository structure
```text
airflow/
  dags/daily_rollup_to_bq.py
docker/
  docker-compose.yml
producer/
  producer.py
spark/
  streaming_job.py
docs/
  images/
```

## Run locally
Prerequisites:
- Docker Desktop
- Google service account JSON placed at `airflow/secret/gcp-sa.json`

Start full stack:
```powershell
docker compose up -d zookeeper kafka producer spark-minute airflow-postgres airflow-init airflow-webserver airflow-scheduler
```

Access services:
- Airflow UI: `http://localhost:8083` (admin/admin)
- Kafka UI: `http://localhost:8081`

Stop everything (safe for overnight shutdown):
```powershell
docker compose stop
```

Start again next day:
```powershell
docker compose up -d
```

## Validation commands
Check raw stream in Kafka:
```powershell
docker exec avax-streaming-data-pipeline-kafka-1 bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic binance_avaxusdt_aggtrades --max-messages 5 --timeout-ms 10000"
```

Check minute parquet records:
```powershell
docker exec avax-streaming-data-pipeline-airflow-scheduler-1 python -c "import pyarrow.dataset as ds; t=ds.dataset('/data/minute', format='parquet').to_table(); print('rows=', t.num_rows); print(t.to_pandas().tail(5).to_string(index=False))"
```

## Results (portfolio section)
Use this section for visual proof in CV/GitHub profile.

1. Pipeline status screenshot (Docker + running services)
![Pipeline Status](docs/images/result-pipeline-status.png)

2. Airflow DAG run screenshot
![Airflow DAG Run](docs/images/result-airflow-dag.png)

3. Kafka message sample screenshot
![Kafka Stream Sample](docs/images/result-kafka-stream.png)

4. Minute parquet sample output screenshot
![Minute Aggregates Sample](docs/images/result-minute-aggregates.png)

5. BigQuery final table screenshot
![BigQuery Daily Output](docs/images/result-bigquery-output.png)

## Notes
- Daily load uses append mode to stay compatible with BigQuery projects where billing is not enabled for DML.
- If billing is enabled, daily deduplication can be switched on for fully idempotent overwrite-by-day behavior.

## Future improvements
- Add dbt layer for warehouse modeling and tests.
- Add Great Expectations or Soda for automated data quality checks.
- Add CI pipeline for linting/tests on every commit.
- Add dashboard (Looker Studio / Metabase) on top of `crypto.avax_daily`.
