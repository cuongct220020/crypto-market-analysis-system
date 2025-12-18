import asyncio
import time
from typing import List, Dict, Any, Set
from tqdm.asyncio import tqdm

from ingestion.web2.coingecko_client import CoinGeckoClient
from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from utils.logger_utils import get_logger
from constants.price_feed_contract_address import PRICE_FEED_CONTRACT_ADDRESS

logger = get_logger("CoinGecko Ingester")

TOPIC_COINS_MARKET = "coingecko.eth.coins.market.v0"
TOPIC_COIN_HISTORICAL = "coingecko.eth.coin.historical.v0"

async def _get_target_eth_coins(cg: CoinGeckoClient) -> List[Dict[str, Any]]:
    """
    Helper function to identify target Ethereum coins based on Price Feed Contracts.
    Returns a list of coin dictionaries (id, symbol, name, eth_contract_address).
    """
    # 1. Get Coin List with Platform info
    logger.info("Fetching coin list with platform info...")
    coins = await cg.get_coin_list(include_platform=True)
    
    if not coins:
        logger.error("Failed to fetch coin list.")
        return []

    # Prepare target IDs based on Price Feed Contracts
    logger.info("Extracting target symbols from Price Feed Contracts...")
    target_symbols = set()
    for key in PRICE_FEED_CONTRACT_ADDRESS.keys():
        # key format: "symbol-symbol" e.g., "eth-usd", "link-eth"
        parts = key.split("-")
        target_symbols.update(parts)
    
    # Filter coins
    target_coins = []
    for coin in coins:
        # Check if symbol matches and is on Ethereum
        if coin["symbol"].lower() in target_symbols:
            platforms = coin.get("platforms", {})
            if "ethereum" in platforms and platforms["ethereum"]:
                 target_coins.append({
                    "id": coin["id"],
                    "symbol": coin["symbol"],
                    "name": coin["name"],
                    "eth_contract_address": platforms["ethereum"]
                })
    
    logger.info(f"Identified {len(target_coins)} target Ethereum coins from {len(target_symbols)} symbols.")
    return target_coins

async def ingest_market_data(kafka_output: str):
    """
    Ingests current market data for Ethereum tokens from CoinGecko.
    """
    producer = KafkaProducerWrapper(kafka_broker_url=kafka_output)
    
    async with CoinGeckoClient() as cg:
        target_coins = await _get_target_eth_coins(cg)
        if not target_coins:
            logger.warning("No target coins found.")
            return

        target_ids = [c["id"] for c in target_coins]
        
        # Fetch coin markets
        logger.info(f"Fetching coin markets for {len(target_ids)} tokens...")
        markets = await cg.get_all_coin_markets(vs_currency="usd", ids=target_ids)
        market_map = {m["id"]: m for m in markets}

        # Enrich and publish
        count = 0
        for coin in target_coins:
            coin_id = coin["id"]
            market_info = market_map.get(coin_id)
            
            if market_info:
                # Merge basic info with market info
                # Note: We prioritize market_info fields but keep our eth_contract_address
                enriched_data = {
                    "id": coin_id,
                    "symbol": coin["symbol"],
                    "name": coin["name"],
                    "eth_contract_address": coin["eth_contract_address"],
                    "image": market_info.get("image"),
                    "current_price": market_info.get("current_price"),
                    "market_cap": market_info.get("market_cap"),
                    "market_cap_rank": market_info.get("market_cap_rank"),
                    "fully_diluted_valuation": market_info.get("fully_diluted_valuation"),
                    "total_volume": market_info.get("total_volume"),
                    "high_24h": market_info.get("high_24h"),
                    "low_24h": market_info.get("low_24h"),
                    "price_change_24h": market_info.get("price_change_24h"),
                    "price_change_percentage_24h": market_info.get("price_change_percentage_24h"),
                    "market_cap_change_24h": market_info.get("market_cap_change_24h"),
                    "market_cap_change_percentage_24h": market_info.get("market_cap_change_percentage_24h"),
                    "circulating_supply": market_info.get("circulating_supply"),
                    "total_supply": market_info.get("total_supply"),
                    "max_supply": market_info.get("max_supply"),
                    "ath": market_info.get("ath"),
                    "ath_change_percentage": market_info.get("ath_change_percentage"),
                    "ath_date": market_info.get("ath_date"),
                    "atl": market_info.get("atl"),
                    "atl_change_percentage": market_info.get("atl_change_percentage"),
                    "atl_date": market_info.get("atl_date"),
                    "roi": market_info.get("roi"),
                    "last_updated": market_info.get("last_updated")
                }
                
                producer.produce(
                    topic=TOPIC_COINS_MARKET,
                    value=enriched_data,
                    schema_key="coin_market",
                    key=coin_id
                )
                count += 1
        
        logger.info(f"Published {count} market data records to {TOPIC_COINS_MARKET}")
        producer.flush()

async def ingest_historical_data(kafka_output: str, days: str = "max", start_ts: int = 1262304000, console_output: bool = False):
    """
    Ingests historical market data (OHLC + Market Chart) for Ethereum tokens from CoinGecko.
    """
    producer = None
    if kafka_output and "kafka" in kafka_output.lower():
        try:
            producer = KafkaProducerWrapper(kafka_broker_url=kafka_output)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            if not console_output:
                return

    import json # Import locally to avoid top-level pollution if preferred, or move to top
    
    async with CoinGeckoClient() as cg:
        target_coins = await _get_target_eth_coins(cg)
        if not target_coins:
            logger.warning("No target coins found.")
            return
            
        # Fetch Historical Data for each coin
        pbar = tqdm(target_coins, desc="Fetching Historical Data")
        
        for coin in pbar:
            coin_id = coin["id"]
            pbar.set_postfix(current_coin=coin_id)
            
            try:
                # Fetch Market Chart (Max)
                now = int(time.time())
                
                market_chart = await cg.get_coin_market_chart_range(
                    coin_id=coin_id,
                    vs_currency="usd",
                    from_timestamp=start_ts,
                    to_timestamp=now
                )
                
                if market_chart is None:
                    logger.warning(f"Skipping {coin_id} due to API failure (market chart).")
                    continue

                # Fetch OHLC
                ohlc_data = await cg.get_coin_ohlc(
                    coin_id=coin_id,
                    vs_currency="usd",
                    days=days
                )
                
                if ohlc_data is None:
                    logger.warning(f"Skipping {coin_id} due to API failure (OHLC).")
                    continue

                # Merge Data
                merged_data = {}
                
                if market_chart:
                    prices = dict(market_chart.get("prices", []))
                    market_caps = dict(market_chart.get("market_caps", []))
                    total_volumes = dict(market_chart.get("total_volumes", []))
                    
                    all_timestamps = set(prices.keys()) | set(market_caps.keys()) | set(total_volumes.keys())
                    
                    for ts in all_timestamps:
                        merged_data[ts] = {
                            "timestamp": ts,
                            "coin_id": coin_id,
                            "price": prices.get(ts),
                            "market_cap": market_caps.get(ts),
                            "total_volume": total_volumes.get(ts),
                            "open": None,
                            "high": None,
                            "low": None,
                            "close": None
                        }

                if ohlc_data:
                    # Sort keys to enable efficient searching or just iterate
                    market_timestamps = sorted(merged_data.keys())
                    
                    for entry in ohlc_data:
                        # entry: [time, open, high, low, close]
                        ts_ohlc = entry[0]
                        open_p = entry[1]
                        high_p = entry[2]
                        low_p = entry[3]
                        close_p = entry[4]
                        
                        # 1. Exact Match
                        if ts_ohlc in merged_data:
                            merged_data[ts_ohlc]["open"] = open_p
                            merged_data[ts_ohlc]["high"] = high_p
                            merged_data[ts_ohlc]["low"] = low_p
                            merged_data[ts_ohlc]["close"] = close_p
                        else:
                            # 2. Fuzzy Match (Find nearest market chart point within 30 mins)
                            # 30 mins = 1800000 ms
                            tolerance = 1800000 
                            closest_ts = None
                            min_diff = float('inf')
                            
                            found_match = False
                            for ts_market in market_timestamps:
                                diff = abs(ts_market - ts_ohlc)
                                if diff <= tolerance:
                                    if diff < min_diff:
                                        min_diff = diff
                                        closest_ts = ts_market
                            
                            if closest_ts:
                                # Move data from closest_ts to ts_ohlc
                                data = merged_data.pop(closest_ts)
                                data["timestamp"] = ts_ohlc # Update timestamp to align with OHLC
                                data["open"] = open_p
                                data["high"] = high_p
                                data["low"] = low_p
                                data["close"] = close_p
                                
                                merged_data[ts_ohlc] = data
                                
                                market_timestamps.remove(closest_ts)
                                found_match = True
                            
                            if not found_match:
                                # No match found, create new
                                merged_data[ts_ohlc] = {
                                    "timestamp": ts_ohlc,
                                    "coin_id": coin_id,
                                    "price": None, 
                                    "market_cap": None,
                                    "total_volume": None,
                                    "open": open_p,
                                    "high": high_p,
                                    "low": low_p,
                                    "close": close_p
                                }
                
                # Publish / Print
                if merged_data:
                    if producer:
                        for ts, data in merged_data.items():
                            producer.produce(
                                topic=TOPIC_COIN_HISTORICAL,
                                value=data,
                                schema_key="coin_historical",
                                key=coin_id
                            )
                        producer.flush(timeout=1)
                    
                    if console_output:
                        tqdm.write(f"\n--- Historical Data for {coin_id} ({len(merged_data)} records) ---")
                        # Print a sample (first 2, last 2) to avoid spamming, or full if user requested debug?
                        # User said "console for debug", so maybe full or truncated. Let's print full JSON for now but compacted?
                        # Actually huge output might hang terminal. Let's print list.
                        tqdm.write(json.dumps(list(merged_data.values()), indent=2))
                        tqdm.write("---------------------------------------------------------------")
                
                # Rate limit sleep
                await asyncio.sleep(5) 
                
            except Exception as e:
                logger.error(f"Error processing {coin_id}: {e}")
                await asyncio.sleep(10) # Backoff on error
        
        if producer:
            producer.flush()
