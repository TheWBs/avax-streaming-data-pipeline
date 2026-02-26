from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, min as fmin, max as fmax, sum as fsum, count as fcount,
    first, last, to_timestamp, when
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType


KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "binance_avaxusdt_aggtrades"

OUTPUT_PATH = "/data/minute"
CHECKPOINT_PATH = "/data/checkpoints/minute"


# Schema of *your* Kafka message (wrapper + Binance payload inside `data`)
binance_data_schema = StructType([
    StructField("e", StringType(), True),
    StructField("E", LongType(), True),
    StructField("s", StringType(), True),
    StructField("a", LongType(), True),
    StructField("p", StringType(), True),   # price as string
    StructField("q", StringType(), True),   # qty as string
    StructField("T", LongType(), True),     # trade time ms
    StructField("m", BooleanType(), True),  # buyer is maker
])

wrapper_schema = StructType([
    StructField("ingest_ts_ms", LongType(), True),
    StructField("source", StringType(), True),
    StructField("stream", StringType(), True),
    StructField("data", binance_data_schema, True),
])


spark = (
    SparkSession.builder
    .appName("avax-minute-aggregates")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), wrapper_schema).alias("msg"))
    .select(
        col("msg.data.s").alias("symbol"),
        (col("msg.data.T") / 1000).cast("timestamp").alias("event_ts"),
        col("msg.data.p").cast("double").alias("price"),
        col("msg.data.q").cast("double").alias("qty"),
        col("msg.data.m").alias("buyer_is_maker"),
    )
    .where(col("symbol").isNotNull() & col("event_ts").isNotNull())
)

# buyer_is_maker == False => taker BUY (buyer is taker)
agg = (
    parsed
    .withWatermark("event_ts", "2 minutes")
    .groupBy(
        col("symbol"),
        window(col("event_ts"), "1 minute").alias("w")
    )
    .agg(
        first("price").alias("open"),
        fmax("price").alias("high"),
        fmin("price").alias("low"),
        last("price").alias("close"),
        fcount("*").alias("trade_count"),
        fsum("qty").alias("base_volume"),
        fsum(when(col("buyer_is_maker") == False, col("qty")).otherwise(0.0)).alias("taker_buy_base_volume"),
        fsum(when(col("buyer_is_maker") == True, col("qty")).otherwise(0.0)).alias("taker_sell_base_volume"),
    )
    .select(
        col("symbol"),
        col("w.start").alias("minute_ts"),
        col("open"), col("high"), col("low"), col("close"),
        col("trade_count"),
        col("base_volume"),
        col("taker_buy_base_volume"),
        col("taker_sell_base_volume"),
        (col("taker_buy_base_volume") / (col("base_volume") + col("base_volume")*0 + 1e-9)).alias("taker_buy_ratio")
    )
)

query = (
    agg.writeStream
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

query.awaitTermination()