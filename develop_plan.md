# Lambda Architecture Integration Guide
## Hệ thống Phân tích Thị trường Crypto

---

## 1. PHÂN TÍCH HIỆN TRẠNG

### 1.1 Những gì đã có ✅

**Ingestion Layer:**
- ✅ Kafka cluster (3 brokers) với Schema Registry
- ✅ CoinGecko ingestion (market + historical data)
- ✅ Ethereum blockchain ingestion (blocks, txs, receipts, contracts)
- ✅ Chainlink price feeds

**Speed Layer (Streaming):**
- ✅ Spark Streaming jobs: `ingest_market_prices.py`, `calculate_trending_metrics.py`, `detect_whale_alerts.py`
- ✅ Elasticsearch indices đã được định nghĩa
- ✅ Kafka → Elasticsearch pipeline

**Storage Layer:**
- ✅ ClickHouse cluster (3 nodes) với Kafka Engine Tables + Materialized Views
- ✅ Auto-ingestion từ Kafka topics vào ClickHouse

**Orchestration:**
- ✅ Airflow DAGs để trigger streaming jobs

### 1.2 Những gì còn thiếu ❌

**Batch Layer:**
- ❌ Spark Batch jobs để process historical data
- ❌ Aggregation tables trong ClickHouse
- ❌ Backfill strategy cho historical metrics
- ❌ DAGs cho batch processing

**Integration:**
- ❌ Cơ chế merge data từ batch và streaming layers
- ❌ Deduplication logic
- ❌ Data reconciliation strategy

---

## 2. KIẾN TRÚC ĐỀ XUẤT: HYBRID LAMBDA

### 2.1 Tại sao Hybrid Lambda?

**Classic Lambda có vấn đề:**
- Duplicate logic giữa batch và streaming
- Complex để maintain 2 codebases
- Data inconsistency giữa 2 layers

**Hybrid Lambda (ClickHouse-centric):**
- ClickHouse Materialized Views tự động xử lý streaming
- Batch layer chỉ dùng cho:
  - Backfill historical data
  - Recompute metrics khi logic thay đổi
  - Complex aggregations không phù hợp với MV
- Single source of truth: ClickHouse

### 2.2 Luồng dữ liệu tổng thể

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
├─────────────────────────────────────────────────────────────────┤
│  CoinGecko API  │  Chainlink  │  Ethereum RPC  │  CMC API      │
└────────┬────────┴──────┬──────┴────────┬───────┴───────────────┘
         │               │               │
         v               v               v
    ┌────────────────────────────────────────┐
    │      INGESTION LAYER (Kafka)           │
    │  Topics: blocks, txs, market, prices   │
    └───┬────────────────────────────────┬───┘
        │                                │
        v                                v
┌───────────────────┐          ┌─────────────────────┐
│   BATCH LAYER     │          │   SPEED LAYER       │
│  Spark Batch      │          │  Spark Streaming    │
│  (Airflow)        │          │  (Airflow)          │
└────┬──────────────┘          └──────┬──────────────┘
     │                                │
     │    ┌───────────────────────────┤
     │    │                           │
     v    v                           v
┌─────────────────────────────────────────────────────┐
│        STORAGE LAYER (ClickHouse)                   │
│  • Raw tables (via Kafka Engine + MV)               │
│  • Batch aggregation tables                         │
│  • Streaming aggregation tables                     │
└────────────────┬────────────────────────────────────┘
                 │
                 v
        ┌────────────────────┐
        │   SERVING LAYER    │
        │  Elasticsearch +   │
        │      Kibana        │
        └────────────────────┘
```

---

## 3. THIẾT KẾ CHI TIẾT

### 3.1 ClickHouse Schema Strategy

#### A. Raw Data Tables (Đã có)
Dữ liệu raw được ingest tự động qua Kafka Engine:
- `blocks`, `transactions`, `receipts`, `logs`, `token_transfers`, `contracts`

#### B. Aggregation Tables (CẦN BỔ SUNG)

**1. Daily Market Metrics**
```sql
CREATE TABLE IF NOT EXISTS daily_market_metrics (
    date Date,
    coin_id String,
    coin_symbol LowCardinality(String),
    
    -- Price metrics
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    
    -- Volume metrics
    total_volume Float64,
    volume_change_24h Float64,
    
    -- Market cap
    market_cap Float64,
    market_cap_rank UInt32,
    
    -- Price changes
    price_change_24h Float64,
    price_change_pct_24h Float64,
    
    -- Metadata
    calculated_at DateTime DEFAULT now(),
    data_source LowCardinality(String) -- 'batch' or 'streaming'
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, coin_id);
```

**2. Hourly Trending Metrics**
```sql
CREATE TABLE IF NOT EXISTS hourly_trending_metrics (
    hour DateTime,
    coin_id String,
    
    -- Price volatility
    price_volatility Float64,
    price_momentum Float64,
    
    -- Volume analysis
    volume_avg Float64,
    volume_spike_ratio Float64, -- Current vs average
    
    -- Transaction activity
    transaction_count UInt64,
    unique_addresses UInt64,
    
    -- Whale activity
    whale_tx_count UInt32,
    whale_volume Float64,
    
    -- Trending score (composite metric)
    trending_score Float64,
    
    calculated_at DateTime DEFAULT now(),
    data_source LowCardinality(String)
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, trending_score DESC, coin_id);
```

**3. Top Gainers/Losers**
```sql
CREATE TABLE IF NOT EXISTS top_movers (
    date Date,
    period_type LowCardinality(String), -- '1h', '24h', '7d'
    coin_id String,
    coin_symbol LowCardinality(String),
    
    price_change_pct Float64,
    volume Float64,
    market_cap Float64,
    
    rank UInt16, -- 1-100
    mover_type LowCardinality(String), -- 'gainer' or 'loser'
    
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, period_type, rank);
```

**4. Protocol Metrics (DeFi)**
```sql
CREATE TABLE IF NOT EXISTS protocol_metrics (
    date Date,
    protocol_address String,
    protocol_name String,
    protocol_category LowCardinality(String), -- 'DEX', 'Lending', etc
    
    -- TVL metrics
    tvl_usd Float64,
    tvl_change_24h Float64,
    
    -- Revenue metrics
    revenue_24h Float64,
    fees_24h Float64,
    
    -- Activity metrics
    active_users_24h UInt64,
    transaction_count_24h UInt64,
    
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, tvl_usd DESC, protocol_address);
```

### 3.2 Batch Processing Jobs

#### Job 1: Daily Market Aggregation
**File:** `processing/batch/aggregate_daily_markets.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def compute_daily_metrics(spark, date_str):
    """
    Tính toán metrics theo ngày từ market data trong ClickHouse.
    """
    # Read from ClickHouse (via JDBC hoặc native connector)
    market_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse-01:8123/crypto") \
        .option("dbtable", """
            (SELECT * FROM crypto_market_prices 
             WHERE toDate(timestamp) = '{date}')
        """.format(date=date_str)) \
        .load()
    
    # Calculate OHLC
    daily_metrics = market_df.groupBy("coin_id", "symbol") \
        .agg(
            first("current_price").alias("open_price"),
            max("current_price").alias("high_price"),
            min("current_price").alias("low_price"),
            last("current_price").alias("close_price"),
            sum("total_volume").alias("total_volume"),
            avg("market_cap").alias("market_cap"),
            last("market_cap_rank").alias("market_cap_rank")
        ) \
        .withColumn("date", lit(date_str)) \
        .withColumn("data_source", lit("batch"))
    
    # Write back to ClickHouse
    daily_metrics.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse-01:8123/crypto") \
        .option("dbtable", "daily_market_metrics") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Daily Market Aggregation") \
        .getOrCreate()
    
    # Process yesterday's data
    from datetime import datetime, timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    compute_daily_metrics(spark, yesterday)
```

#### Job 2: Hourly Trending Analysis
**File:** `processing/batch/calculate_trending_scores.py`

```python
def calculate_trending_score(spark, hour_str):
    """
    Tính trending score dựa trên:
    - Price volatility (30%)
    - Volume spike (25%)
    - Transaction activity (20%)
    - Whale activity (15%)
    - Social signals (10%) - future
    """
    # Read hourly data
    hourly_df = spark.read.jdbc(...)
    
    # Calculate components
    trending_df = hourly_df \
        .withColumn("price_volatility", 
            (col("high_price") - col("low_price")) / col("low_price")) \
        .withColumn("volume_spike_ratio", 
            col("current_volume") / col("avg_volume_7d")) \
        .withColumn("whale_impact",
            col("whale_volume") / col("total_volume"))
    
    # Composite score
    trending_df = trending_df \
        .withColumn("trending_score",
            col("price_volatility") * 0.3 +
            col("volume_spike_ratio") * 0.25 +
            col("tx_growth") * 0.2 +
            col("whale_impact") * 0.15 +
            lit(0) * 0.1  # Reserved for social signals
        )
    
    # Top 100
    window_spec = Window.orderBy(col("trending_score").desc())
    top_trending = trending_df \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 100)
    
    top_trending.write.jdbc(...)
```

#### Job 3: Protocol TVL & Revenue
**File:** `processing/batch/aggregate_protocol_metrics.py`

```python
def aggregate_protocol_metrics(spark, date_str):
    """
    Aggregate DeFi protocol metrics:
    - TVL from token balances
    - Revenue from transaction fees
    - Active users from unique addresses
    """
    # Identify DeFi contracts
    defi_contracts = spark.read.jdbc(
        url="...",
        table="""
        (SELECT address, name, impl_category 
         FROM contracts 
         WHERE impl_category IN ('FACTORY', 'ROUTER', 'VAULT'))
        """
    )
    
    # Get transactions to these contracts
    protocol_txs = spark.read.jdbc(
        url="...",
        table=f"""
        (SELECT to_address, from_address, value, receipt_gas_used
         FROM transactions 
         WHERE toDate(block_timestamp) = '{date_str}')
        """
    )
    
    # Calculate metrics
    protocol_metrics = protocol_txs.join(
        defi_contracts, 
        protocol_txs.to_address == defi_contracts.address
    ).groupBy("to_address", "name") \
        .agg(
            countDistinct("from_address").alias("active_users_24h"),
            count("*").alias("transaction_count_24h"),
            sum("value").alias("total_volume")
        )
    
    # Write results
    protocol_metrics.write.jdbc(...)
```

### 3.3 Streaming Enhancement

Hiện tại bạn đã có `ingest_market_prices.py`. Cần enhance để:

**1. Tính toán real-time metrics**
```python
# processing/streaming/ingest_market_prices.py (enhanced)

from pyspark.sql.functions import window, lag
from pyspark.sql.window import Window

# Existing code...
processed_stream = parsed_stream \
    .withColumn("timestamp", to_timestamp(col("last_updated"))) \
    .withColumnRenamed("id", "coin_id")

# ADD: Calculate real-time changes
window_spec = Window.partitionBy("coin_id").orderBy("timestamp")
enriched_stream = processed_stream \
    .withColumn("prev_price", lag("current_price", 1).over(window_spec)) \
    .withColumn("price_change_1m", 
        (col("current_price") - col("prev_price")) / col("prev_price") * 100) \
    .withColumn("data_source", lit("streaming"))

# Write to BOTH Elasticsearch (real-time) AND ClickHouse (historical)
# To Elasticsearch (existing)
query_es = enriched_stream.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option(...) \
    .start()

# To ClickHouse (NEW)
query_ch = enriched_stream.writeStream \
    .outputMode("append") \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://clickhouse-01:8123/crypto") \
    .option("dbtable", "streaming_market_updates") \
    .option("checkpointLocation", "/opt/spark/checkpoints/market_ch") \
    .start()
```

**2. Real-time trending detection**
```python
# processing/streaming/calculate_trending_metrics.py (enhanced)

# Calculate sliding window metrics
trending_metrics = market_parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),  # 5-min window, 1-min slide
        col("coin_id")
    ) \
    .agg(
        avg("current_price").alias("avg_price"),
        stddev("current_price").alias("price_volatility"),
        sum("total_volume").alias("volume_5m")
    ) \
    .withColumn("trending_score", 
        col("price_volatility") * 0.5 + 
        col("volume_5m") / 1000000 * 0.5
    )
```

### 3.4 Airflow DAGs

#### DAG 1: Daily Batch Processing
**File:** `airflow/dags/batch_daily_aggregation_dag.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'batch_daily_aggregation',
    default_args=default_args,
    description='Daily batch aggregation of market metrics',
    schedule_interval='0 1 * * *',  # Run at 1 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch', 'aggregation', 'daily'],
) as dag:

    aggregate_daily_markets = SparkSubmitOperator(
        task_id='aggregate_daily_markets',
        application='/opt/airflow/project/processing/batch/aggregate_daily_markets.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '4g',
        }
    )
    
    calculate_trending_scores = SparkSubmitOperator(
        task_id='calculate_trending_scores',
        application='/opt/airflow/project/processing/batch/calculate_trending_scores.py',
        conn_id='spark_default'
    )
    
    aggregate_protocol_metrics = SparkSubmitOperator(
        task_id='aggregate_protocol_metrics',
        application='/opt/airflow/project/processing/batch/aggregate_protocol_metrics.py',
        conn_id='spark_default'
    )

    # Dependencies
    aggregate_daily_markets >> [calculate_trending_scores, aggregate_protocol_metrics]
```

#### DAG 2: Hourly Batch Processing
```python
with DAG(
    'batch_hourly_aggregation',
    default_args=default_args,
    schedule_interval='0 * * * *',  # Every hour
    ...
) as dag:
    # Similar structure for hourly jobs
    pass
```

#### DAG 3: Backfill Historical Data
```python
with DAG(
    'batch_backfill_historical',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    ...
) as dag:
    # Backfill logic for historical date ranges
    pass
```

---

## 4. SERVING LAYER STRATEGY

### 4.1 Query Routing Logic

**Real-time queries (< 1 hour old)** → Elasticsearch
```python
# app/api/realtime_metrics.py
def get_trending_now():
    """Get trending coins in last 5 minutes"""
    es_client = Elasticsearch(...)
    query = {
        "query": {
            "range": {
                "calculated_at": {
                    "gte": "now-5m"
                }
            }
        },
        "sort": [{"trending_score": "desc"}],
        "size": 10
    }
    return es_client.search(index="crypto_trending_metrics", body=query)
```

**Historical queries (> 1 hour old)** → ClickHouse
```python
# app/api/historical_metrics.py
def get_top_gainers_24h(date):
    """Get top gainers for a specific date"""
    query = f"""
    SELECT coin_id, coin_symbol, price_change_pct, volume
    FROM top_movers
    WHERE date = '{date}' 
      AND period_type = '24h'
      AND mover_type = 'gainer'
    ORDER BY rank ASC
    LIMIT 10
    """
    return clickhouse_client.execute(query)
```

**Hybrid queries (last 24 hours)** → Union from both
```python
def get_trending_24h():
    """Combine recent streaming + batch data"""
    # Last 1 hour from Elasticsearch
    recent = get_trending_from_es("now-1h")
    
    # Previous 23 hours from ClickHouse
    historical = clickhouse_client.execute("""
        SELECT * FROM hourly_trending_metrics
        WHERE hour >= now() - INTERVAL 24 HOUR
          AND hour < now() - INTERVAL 1 HOUR
        ORDER BY hour DESC, trending_score DESC
    """)
    
    # Merge and re-rank
    combined = merge_and_rank(recent, historical)
    return combined
```

### 4.2 Deduplication Strategy

Vì cả batch và streaming đều ghi vào ClickHouse, dùng `ReplacingMergeTree`:

```sql
-- ClickHouse tự động deduplicate dựa trên ORDER BY keys
-- Giữ record mới nhất dựa trên version column (calculated_at)

SELECT * FROM daily_market_metrics FINAL
WHERE date = '2024-01-01' AND coin_id = 'bitcoin'
-- FINAL keyword forces deduplication
```

---

## 5. DEPLOYMENT & MONITORING

### 5.1 Resource Allocation

**Spark Streaming Jobs:**
- Driver: 1GB RAM
- Executor: 2GB RAM x 2 executors
- Cores: 2 per executor
- Run continuously via Airflow

**Spark Batch Jobs:**
- Driver: 2GB RAM
- Executor: 4GB RAM x 3 executors
- Cores: 4 per executor
- Scheduled via Airflow

### 5.2 Monitoring Metrics

**Data Freshness:**
```sql
-- Check data lag in ClickHouse
SELECT 
    table_name,
    max(block_timestamp) as latest_block_ts,
    now() - toDateTime(max(block_timestamp)) as lag_seconds
FROM (
    SELECT 'blocks' as table_name, max(timestamp) as block_timestamp FROM blocks
    UNION ALL
    SELECT 'transactions', max(block_timestamp) FROM transactions
    UNION ALL
    SELECT 'market_prices', max(timestamp) FROM crypto_market_prices
) GROUP BY table_name
```

**Processing Health:**
- Airflow DAG success rate
- Spark job duration trends
- Kafka consumer lag
- Elasticsearch indexing rate

---

## 6. IMPLEMENTATION ROADMAP

### Phase 1: Foundation (Week 1-2)
- [ ] Create ClickHouse aggregation tables
- [ ] Implement Job 1: Daily market aggregation
- [ ] Create Airflow DAG for daily batch
- [ ] Test batch → ClickHouse pipeline

### Phase 2: Core Metrics (Week 3-4)
- [ ] Implement Job 2: Trending scores
- [ ] Implement Job 3: Protocol metrics
- [ ] Enhance streaming jobs to write to ClickHouse
- [ ] Implement deduplication logic

### Phase 3: Integration (Week 5-6)
- [ ] Build API layer với query routing
- [ ] Create Kibana dashboards
- [ ] Implement data reconciliation
- [ ] Set up monitoring & alerts

### Phase 4: Optimization (Week 7-8)
- [ ] Performance tuning ClickHouse queries
- [ ] Optimize Spark job resources
- [ ] Implement caching layer
- [ ] Load testing & benchmarking

---

## 7. BEST PRACTICES

### 7.1 Data Consistency
- Use idempotent writes (ReplacingMergeTree)
- Include `data_source` column to track origin
- Implement reconciliation jobs to compare batch vs streaming

### 7.2 Performance
- Partition ClickHouse tables by month
- Use appropriate ORDER BY keys for query patterns
- Materialize frequently-used aggregations
- Leverage ClickHouse's incremental MV updates

### 7.3 Cost Optimization
- Archive old data to cold storage (S3)
- Use ClickHouse TTL for auto-cleanup
- Right-size Spark executors based on workload
- Schedule batch jobs during off-peak hours

---

## 8. EXAMPLE QUERIES

### Top 10 Trending Coins (Last Hour)
```sql
SELECT 
    coin_id,
    coin_symbol,
    trending_score,
    price_volatility,
    volume_spike_ratio
FROM hourly_trending_metrics FINAL
WHERE hour = toStartOfHour(now() - INTERVAL 1 HOUR)
ORDER BY trending_score DESC
LIMIT 10
```

### Top Gainers (24h)
```sql
SELECT 
    coin_id,
    coin_symbol,
    price_change_pct,
    volume,
    rank
FROM top_movers FINAL
WHERE date = today()
  AND period_type = '24h'
  AND mover_type = 'gainer'
ORDER BY rank ASC
LIMIT 10
```

### Protocol Rankings by TVL
```sql
SELECT 
    protocol_name,
    protocol_category,
    tvl_usd,
    revenue_24h,
    active_users_24h
FROM protocol_metrics FINAL
WHERE date = today()
ORDER BY tvl_usd DESC
LIMIT 20
```

---

## TÓM TẮT

**Kiến trúc Hybrid Lambda này:**
1. ✅ Tận dụng ClickHouse làm single source of truth
2. ✅ Streaming xử lý real-time, Batch xử lý historical/complex
3. ✅ Tránh duplicate logic qua ReplacingMergeTree
4. ✅ Linh hoạt scale từng layer độc lập
5. ✅ Phù hợp với schema hiện tại của bạn

**Workflow thực tế:**
- Real-time: Kafka → Spark Streaming → Elasticsearch + ClickHouse
- Historical: ClickHouse → Spark Batch → ClickHouse (aggregated)
- Serving: Route queries dựa trên data age (ES vs CH)