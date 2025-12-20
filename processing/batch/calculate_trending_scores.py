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