# AVAX Streaming Data Pipeline (Kafka + Spark + Airflow + BigQuery)

End-to-end data engineering project that ingests live **AVAXUSDT** aggTrades from Binance WebSocket, streams them through **Kafka**, computes **minute-level OHLC + volume** with **Spark Structured Streaming**, and loads **daily rollups** into **BigQuery**.

## Stack
- Binance Spot WebSocket (`AVAXUSDT@aggTrade`)
- Apache Kafka + ZooKeeper
- Apache Spark Structured Streaming
- Apache Airflow
- Google BigQuery
- Docker + Docker Compose

## BigQuery target
- `crypto.avax_daily`

## High-level flow
Binance WS -> Producer -> Kafka -> Spark minute parquet (`/data/minute`) -> Airflow daily rollup -> BigQuery `crypto.avax_daily`

## Run (from repository root)
1. Start Airflow components:
   `docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler`
2. Start streaming components:
   `docker compose up -d zookeeper kafka producer spark-minute`
3. Open services:
   - Airflow UI: `http://localhost:8080` (`admin` / `admin`)
   - Kafka UI: `http://localhost:8081`

## Credentials
- Service account JSON is mounted from `airflow/secret/gcp-sa.json` to `/opt/airflow/secrets/gcp-sa.json` inside Airflow containers.