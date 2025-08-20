# CDC Pipeline: PostgreSQL → Kafka → Spark → Cassandra + Redis + REST API

## Overview
This project demonstrates a **Change Data Capture (CDC) pipeline** where:
- **Debezium** captures changes from PostgreSQL (`engagement_events`).
- **Kafka** transports events.
- **Spark Structured Streaming** enriches, cleans, and fans out to multiple sinks:
  - **Cassandra** → historical truth store.
  - **Redis** → real-time aggregates & counters.
  - **REST API** → external microservice + Dead Letter Queue (DLQ) handling.

---

## ⚙ 1. Setup Steps

### PostgreSQL
```sql
CREATE DATABASE engagement_db;
\c engagement_db
CREATE TABLE content (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    content_type TEXT NOT NULL,
    length_seconds INT,
    publish_ts TIMESTAMP NOT NULL
);

CREATE TABLE engagement_events (
    id SERIAL PRIMARY KEY,
    content_id UUID REFERENCES content(id),
    user_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    duration_ms INT,
    device TEXT,
    raw_payload JSONB
);

CREATE PUBLICATION engagement_pub FOR TABLE engagement_events;
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

### Kafka + Debezium Connector
```bash
# Start Zookeeper & Kafka
sudo /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sudo /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

# Create topic
sudo /opt/kafka/bin/kafka-topics.sh --create \
  --topic engagement_project1.public.engagement_events \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Start Kafka Connect + Register Debezium Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @configs/debezium-postgres.json
```

### Cassandra
```sql
CREATE KEYSPACE engagement
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE engagement.engagement_events (
    id int PRIMARY KEY,
    content_id uuid,
    user_id uuid,
    event_type text,
    event_ts timestamp,
    duration_ms int,
    device text,
    raw_payload text,
    engagement_pct float
);
```

### Redis
```bash
sudo apt install redis-server -y
redis-cli ping   # PONG
```

---

## 🖥 2. Spark Job

Run the pipeline:
```bash
spark-submit --packages org.postgresql:postgresql:42.6.0 \
 spark_cdc_multi_sink.py
```

---

##  3. Verification

### Cassandra
```sql
SELECT * FROM engagement.engagement_events LIMIT 10;
```

### Redis
```bash
redis-cli ZRANGE engagement_realtime_counts 0 -1 WITHSCORES

redis-cli ZRANGE engagement_10m 0 -1 WITHSCORES

redis-cli ZRANGE engagement_realtime_avg_pct 0 -1 WITHSCORES

```

### REST API DLQ
```bash
cat /tmp/restapi_dlq.log
```

---

##  Features
- **Real-time fan-out**: Cassandra (historical), Redis (real-time), REST API.
- **DLQ handling**: Bad/failed REST events logged to `/tmp/restapi_dlq.log`.
- **Engagement metric**: `engagement_pct = duration_ms / (length_seconds * 1000)`.
- **Device insights**: Mobile (extremes), Desktop (moderate), Tablet (high commitment).

