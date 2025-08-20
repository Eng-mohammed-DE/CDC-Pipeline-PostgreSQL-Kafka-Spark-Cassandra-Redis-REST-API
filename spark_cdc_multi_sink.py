from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count as spark_count
from pyspark.sql.types import *
import redis
import requests
import time
import uuid
import logging
from decimal import Decimal, ROUND_HALF_UP
from cassandra.cluster import Cluster

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc-pipeline")

# ---------------------------
# Redis client
# ---------------------------
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# ---------------------------
# REST API + DLQ
# ---------------------------
def dlq_write(event_dict, reason):
    with open("/tmp/restapi_dlq.log", "a") as f:
        f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} | reason={reason} | event={event_dict}\n")

def send_to_rest_with_dlq(event_dict, timeout=2):
    try:
        resp = requests.post("http://localhost:5000/api", json=event_dict, timeout=timeout)
        if resp.status_code != 200:
            dlq_write(event_dict, f"status_{resp.status_code}")
            logger.warning("REST API returned non-200: %s", resp.status_code)
    except Exception as e:
        logger.exception("REST API Exception")
        dlq_write(event_dict, str(e))

# ---------------------------
# Spark initialization
# ---------------------------
spark = SparkSession.builder \
    .appName("CDC-MultiSink-Fanout") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Kafka payload schema
# ---------------------------
payload_schema = StructType([
    StructField("id", LongType()),
    StructField("content_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("duration_ms", IntegerType()),
    StructField("device", StringType()),
    StructField("raw_payload", StringType())
])

top_schema = StructType([
    StructField("schema", StringType()),
    StructField("payload", payload_schema)
])

# ---------------------------
# Kafka source
# ---------------------------
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "engagement_project1.public.engagement_events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

events = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), top_schema).alias("data")) \
    .select("data.payload.*") \
    .withColumn("event_ts", to_timestamp(col("event_ts")))

# ---------------------------
# Cassandra wrapper
# ---------------------------
class CassandraSink:
    def __init__(self, hosts=["127.0.0.1"], keyspace="engagement"):
        cluster = Cluster(hosts)
        self.session = cluster.connect(keyspace)
        self.insert_event_stmt = self.session.prepare("""
            INSERT INTO engagement_events
            (id, content_id, user_id, event_type, event_ts, duration_ms, device, engagement_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
    def write_event(self, id, content_id, user_id, event_type, event_ts, duration_ms, device, engagement_pct):
        content_uuid = uuid.UUID(content_id) if content_id else None
        user_uuid = uuid.UUID(user_id) if user_id else None
        self.session.execute(self.insert_event_stmt, (
            id, content_uuid, user_uuid, event_type, event_ts, duration_ms, device, engagement_pct
        ))

cass = CassandraSink()

# ---------------------------
# Load PostgreSQL table via Spark
# ---------------------------
def load_postgres_table():
    jdbc_url = "jdbc:postgresql://localhost:5432/engagement_db"
    properties = {"user": "mohammed", "password": "master", "driver": "org.postgresql.Driver"}
    df = spark.read.jdbc(url=jdbc_url, table="content", properties=properties)
    # We need only content_id -> length_seconds mapping
    return df.select(col("id").alias("content_id"), "length_seconds")

content_df = load_postgres_table()

# ---------------------------
# Enrichment + Multi-sink writes
# ---------------------------
def process_enrich_and_write(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Join with PostgreSQL content lengths
    enriched_df = batch_df.join(content_df, on="content_id", how="left")

    rows = enriched_df.collect()
    logger.info("Processing batch id=%s rows=%d", batch_id, len(rows))

    for r in rows:
        event_pk = r.id if r.id is not None else int(time.time() * 1000)

        # Compute engagement_pct
        engagement_pct = None
        if r.duration_ms is not None and r.length_seconds:
            try:
                engagement_seconds = Decimal(r.duration_ms) / Decimal(1000)
                engagement_pct = float(
                    (engagement_seconds / Decimal(r.length_seconds)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                )
            except Exception:
                logger.exception("Error computing engagement_pct")

        # Cassandra write
        try:
            cass.write_event(event_pk, r.content_id, r.user_id, r.event_type, r.event_ts,
                             r.duration_ms, r.device, engagement_pct)
        except Exception:
            logger.exception("Cassandra write failed")

        # Redis updates
        try:
            if r.content_id:
                redis_client.zincrby("engagement_realtime_counts", 1, r.content_id)
                if engagement_pct is not None:
                    redis_client.hincrbyfloat("engagement_pct_sum", r.content_id, engagement_pct)
                    redis_client.hincrby("engagement_pct_count", r.content_id, 1)
                    sum_val = float(redis_client.hget("engagement_pct_sum", r.content_id) or 0)
                    cnt_val = int(redis_client.hget("engagement_pct_count", r.content_id) or 0)
                    if cnt_val > 0:
                        redis_client.zadd("engagement_realtime_avg_pct", {r.content_id: sum_val / cnt_val})
        except Exception:
            logger.exception("Redis update failed")

        # REST API
        event_dict = {
            "id": event_pk,
            "content_id": r.content_id,
            "user_id": r.user_id,
            "event_type": r.event_type,
            "event_ts": str(r.event_ts) if r.event_ts else None,
            "duration_ms": r.duration_ms,
            "device": r.device,
            "engagement_pct": engagement_pct
        }
        send_to_rest_with_dlq(event_dict)

enrich_query = events.writeStream \
    .foreachBatch(process_enrich_and_write) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/enrich") \
    .trigger(processingTime='1 second') \
    .start()

# ---------------------------
# 10-minute windowed aggregation → Redis
# ---------------------------
agg_df = events \
    .withWatermark("event_ts", "11 minutes") \
    .groupBy(window(col("event_ts"), "10 minutes"), col("content_id")) \
    .agg(spark_count("*").alias("event_count")) \
    .select(col("content_id"), col("event_count"))

def write_agg_to_redis(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        if r.content_id:
            try:
                redis_client.zadd("engagement_10m", {r.content_id: int(r.event_count or 0)})
            except Exception:
                logger.exception("Failed to write aggregation to Redis")

agg_query = agg_df.writeStream \
    .foreachBatch(write_agg_to_redis) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/aggg") \
    .trigger(processingTime='1 second') \
    .start()

spark.streams.awaitAnyTermination()



#  spark-submit   --master local[*]   --jars /home/eng-mohammed/postgresql-42.2.5.jar /home/eng-mohammed/airflow_orchestration/venv3.10/include/spark_cdc_multi_sink.py