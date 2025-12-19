# Real-time OHLC & Volume Spike Detection Plan

## 1. Problem Definition
The goal is to calculate **Real-time OHLC (Open, High, Low, Close) candles** and detect **Volume Spikes** (Pump/Dump signals) for Ethereum tokens.

**Target Metrics:**
*   **Timeframe:** 1 Minute (1m).
*   **Metrics:** Open, High, Low, Close Price, Total Volume.
*   **Alert:** Volume Spike (Current Volume > 3x Avg Volume of last 10 candles).

## 2. Data Source Analysis

We have two primary data sources available in Kafka:

1.  **`coingecko.eth.coins.market.v0`** (Source: CoinGecko API)
    *   *Update Frequency:* Every 5 minutes (Batch-based).
    *   *Suitability:* **LOW** for 1-minute real-time candles. It provides snapshot data (current_price, high_24h, etc.) which is already aggregated and delayed. It cannot be used to construct granular 1m candles from raw trades.

2.  **`ethereum.token_transfer.v0`** (Source: Blockchain RPC - On-chain Logs)
    *   *Update Frequency:* Real-time (per block/log).
    *   *Content:* `token_address`, `from`, `to`, `value` (amount), `transaction_hash`.
    *   *Suitability:* **HIGH** for Volume analysis, but **INSUFFICIENT** for Price candles (OHLC) directly.
        *   *Limitation:* A standard ERC-20 `Transfer` event only contains the *amount* of tokens moved, NOT the *price* at which they were traded.
        *   *DEX Context:* To get the *price*, we need to parse **DEX Swap Events** (e.g., Uniswap `Swap`) instead of generic `Transfer` events.

### Conclusion on Feasibility
With the current schemas (`TokenTransfer` only), we **CANNOT** build accurate OHLC *Price* candles because we lack price data for individual on-chain transactions.

**However, we CAN implement:**
1.  **OHLC *Volume* Candles:** Track the flow of tokens on-chain (Open/Close is meaningless, but High/Low/Sum of transfer amounts is valid).
2.  **Volume Spike Detection:** Based on the aggregated transfer volume.

**To fully implement Price OHLC in the future, we would need to:**
*   Ingest **Uniswap V2/V3 Swap Events**.
*   Calculate price based on `amount0In`, `amount1Out` and the pool reserves.

## 3. Implementation Plan (Adjusted for Current Data)

We will proceed with a **Volume-focused Analytics** approach using `ethereum.token_transfer.v0`, which is valuable for "Whale Alert" systems.

### 3.1. Speed Layer (Spark Structured Streaming)

**File:** `processing/streaming/calculate_volume_candles.py`

**Logic:**
1.  **Read Stream:** From Kafka topic `ethereum.token_transfer.v0`.
2.  **Watermark:** Handle late data (e.g., 10 minutes tolerance).
3.  **Window Aggregation:**
    *   Window: 1 minute (Tumbling).
    *   Group By: `contract_address` (Token).
    *   Aggregates:
        *   `total_volume`: Sum of `value` (normalized).
        *   `tx_count`: Count of transfers.
        *   `max_tx_value`: Max transfer size in that minute.
4.  **Stateful Processing (For Anomaly Detection):**
    *   Use `mapGroupsWithState` (or logical equivalent in SQL) to keep a moving average of the last 10 minutes' volume.
    *   Compare `current_window_volume` vs `avg_volume_last_10`.
    *   Flag `is_anomaly = True` if `current > 3 * avg`.
5.  **Sink:**
    *   Write to Elasticsearch index `crypto_volume_candles_1m`.

### 3.2. Storage Layer (Elasticsearch)

**Index:** `crypto_volume_candles_1m`

**Mapping:**
```json
{
  "mappings": {
    "properties": {
      "window_start": { "type": "date" },
      "window_end": { "type": "date" },
      "token_address": { "type": "keyword" },
      "total_volume": { "type": "double" },
      "tx_count": { "type": "integer" },
      "is_anomaly": { "type": "boolean" },
      "anomaly_score": { "type": "float" }
    }
  }
}
```

### 3.3. Visualization (Kibana)
*   **Bar Chart:** Volume per minute for specific tokens.
*   **Alert Table:** List of tokens with `is_anomaly = true` sorted by time.

## 4. Future Roadmap: True Price OHLC
To achieve the original goal of *Price* OHLC, we must:
1.  Update `ingestion` pipeline to fetch and parse **Uniswap V2 Pair `Swap` events** (Topic `0xd78ad95f...`).
2.  Create a new schema `DEXTrade` containing `price_usd` derived from the swap ratio and ETH price.
3.  Feed this `DEXTrade` topic into the Spark Job.
