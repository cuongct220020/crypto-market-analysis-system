# Thiết Kế Hệ Thống Real-time Analytics với Elasticsearch & Kibana

## 1. Tổng quan Kiến Trúc
Tài liệu này mô tả chi tiết thiết kế cho **Speed Layer** và **Serving Layer** trong kiến trúc Lambda của hệ thống.
Mục tiêu là cung cấp khả năng phân tích và trực quan hóa dữ liệu thị trường Crypto gần như thời gian thực (near real-time).

**Luồng dữ liệu:**
`Ingestion (Kafka)` -> `Processing (Spark Structured Streaming)` -> `Indexing (Elasticsearch)` -> `Visualization (Kibana)`

## 2. Chiến lược Indexing trên Elasticsearch

Để tối ưu cho các truy vấn tổng hợp (Aggregation) và tìm kiếm (Search), dữ liệu sẽ được tổ chức thành các Index Streams (Data Streams) hoặc Time-based Indices.

### 2.1. Index: `crypto_market_prices` (Dữ liệu giá Real-time)
*   **Mục đích:** Phục vụ biểu đồ giá (Line chart), tính toán biến động (Volatility) và phát hiện trend ngắn hạn.
*   **Nguồn dữ liệu:** Topic `coingecko.eth.coins.market.v0` và Chainlink Price Feed.
*   **Mapping:**
    ```json
    {
      "mappings": {
        "properties": {
          "timestamp": { "type": "date" },
          "coin_id": { "type": "keyword" },
          "symbol": { "type": "keyword" },
          "name": { "type": "text" },
          "current_price": { "type": "double" },
          "market_cap": { "type": "long" },
          "total_volume": { "type": "long" },
          "price_change_24h": { "type": "double" },
          "price_change_percentage_24h": { "type": "double" }
        }
      }
    }
    ```

### 2.2. Index: `crypto_token_transfers` (Dòng tiền On-chain)
*   **Mục đích:** Phân tích dòng tiền (Money Flow), phát hiện ví cá mập (Whale Watch), và hoạt động sôi nổi của các Token.
*   **Nguồn dữ liệu:** Topic `ethereum.token_transfer.v0` (đã được enrich).
*   **Mapping:**
    ```json
    {
      "mappings": {
        "properties": {
          "block_timestamp": { "type": "date" },
          "token_address": { "type": "keyword" },
          "token_symbol": { "type": "keyword" },  // Cần join/enrichment để có field này
          "from_address": { "type": "keyword" },
          "to_address": { "type": "keyword" },
          "value_normalized": { "type": "double" }, // Value đã chia cho decimals
          "transaction_hash": { "type": "keyword" },
          "log_index": { "type": "integer" }
        }
      }
    }
    ```

### 2.3. Index: `crypto_trending_metrics` (Dữ liệu tổng hợp)
*   **Mục đích:** Lưu trữ các chỉ số đã được tính toán trước (Pre-calculated metrics) bởi Spark để Kibana hiển thị nhanh mà không cần aggregate lại từ raw data.
*   **Nguồn dữ liệu:** Kết quả của Spark Window Aggregations.
*   **Fields:** `window_start`, `window_end`, `coin_id`, `avg_price`, `total_volume_1h`, `transaction_count_1h`.

## 3. Thiết kế Spark Streaming Job (`processing/streaming/`)

Chúng ta sẽ xây dựng các Spark Job chuyên biệt để đẩy dữ liệu vào ES. Thư viện sử dụng: `elasticsearch-hadoop` (hoặc `elasticsearch-spark-30`).

### 3.1. Job: `ingest_market_prices_to_es.py`
*   **Input:** Kafka Topic `coingecko.eth.coins.market.v0`.
*   **Logic:**
    1.  Đọc stream từ Kafka.
    2.  Parse JSON (theo Schema `CoinMarket`).
    3.  Chuyển đổi `last_updated` thành Timestamp chuẩn.
    4.  Ghi vào Elasticsearch Sink.
*   **Cấu hình Sink:**
    ```python
    df.writeStream \
      .format("org.elasticsearch.spark.sql") \
      .option("es.nodes", "elasticsearch") \
      .option("es.port", "9200") \
      .option("es.resource", "crypto_market_prices") \
      .option("checkpointLocation", "/tmp/checkpoints/es_market_prices") \
      .start()
    ```

### 3.2. Job: `calculate_trending_coins.py` (Phân tích nâng cao)
*   **Input:** Kafka Topic `ethereum.token_transfer.v0` VÀ `coingecko.eth.coins.market.v0`.
*   **Logic:**
    1.  **Watermarking:** Xử lý dữ liệu đến muộn (Late data).
    2.  **Windowing:** Gom nhóm dữ liệu theo cửa sổ thời gian (ví dụ: Sliding Window 1 giờ, trượt mỗi 10 phút).
    3.  **Aggregation:**
        *   `count(tx_hash)` as `activity_level`
        *   `sum(value_normalized)` as `volume_onchain`
    4.  **Ranking:** Top 10 coin có volume thay đổi mạnh nhất.
    5.  Ghi kết quả vào Index `crypto_trending_metrics`.

## 4. Thiết kế Dashboard Kibana (Trực quan hóa)

Dựa trên các Index trên, ta sẽ xây dựng Dashboard "Crypto Market Pulse":

### 4.1. Panel: Market Overview (24h)
*   **Metric:** Tổng vốn hóa thị trường (Sum Market Cap).
*   **Metric:** Tổng Volume giao dịch (Sum Volume).
*   **Gauge:** Chỉ số Sợ hãi & Tham lam (Fear & Greed - nếu có data nguồn).

### 4.2. Panel: Top Trending Coins
*   **Table:** Top 10 Gainers (Sắp xếp theo `price_change_percentage_24h` giảm dần).
*   **Tag Cloud:** Các Token được nhắc đến/giao dịch nhiều nhất (Dựa trên `crypto_token_transfers` count).

### 4.3. Panel: Real-time Price Action
*   **Line Chart:** Biểu đồ giá của BTC, ETH (update mỗi phút).
*   **Bar Chart:** Volume giao dịch theo từng giờ.

### 4.4. Panel: On-chain Insights
*   **Heatmap:** Mức độ hoạt động của các Token trên mạng lưới Ethereum (x: Time, y: Token, Color: Transaction Count).
*   **Donut Chart:** Phân bổ dòng tiền vào các mảng (DeFi, Layer 2, Meme - cần phân loại Category).

## 5. Các bước triển khai tiếp theo
1.  **Cài đặt thư viện:** Thêm `elasticsearch-spark-30_2.12` vào Spark Cluster Dockerfile hoặc submit packages.
2.  **Tạo Index Template:** Viết script Python (dùng `elasticsearch-py`) hoặc Dev Tools query để khởi tạo mapping trong ES.
3.  **Coding Spark Job:** Implement `processing/streaming/ingest_to_es.py`.
4.  **Cấu hình Kibana:** Kết nối Index Pattern và vẽ Dashboard.
