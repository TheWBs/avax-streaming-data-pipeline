# AVAX Streaming Data Pipeline (Kafka + Spark + Airflow + BigQuery)

End-to-end data engineering project that ingests live **AVAXUSDT** trade data from Binance WebSocket, streams it through **Kafka**, computes **minute-level market aggregates** with **Spark Structured Streaming**, and loads **aggregated metrics** into **BigQuery** for analytics. **Airflow** orchestrates daily rollups, data quality checks, and backfills.

## Stack
- Binance Spot WebSocket (`AVAXUSDT@aggTrade`)
- Apache Kafka (stream ingestion)
- Apache Spark Structured Streaming (minute aggregates)
- Apache Airflow (daily rollups + DQ + backfills)
- Google BigQuery (analytics warehouse for aggregates only)
- Docker + Docker Compose (local reproducible environment)

## Output Tables (BigQuery)
- `market_metrics_minute`
- `market_metrics_daily`

## Architecture (high level)
Binance WS → Producer → Kafka → Spark Streaming → (aggregates) → Airflow loads → BigQuery