import asyncio
import time
from typing import List, Dict, Any
from tqdm.asyncio import tqdm

from ingestion.web2.coingecko_client import CoinGeckoClient
from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from utils.logger_utils import get_logger

logger = get_logger("CoinGecko Ingester")

TOPIC_COINS_MARKET = "coingecko.eth.coins.market.v0"
TOPIC_COIN_HISTORICAL = "coingecko.eth.coin.historical.v0"

async def _get_target_eth_coins(cg: CoinGeckoClient, top_n: int = 250) -> List[Dict[str, Any]]:
    """
    Identifies target Ethereum coins based on Market Cap ranking (Top N).
    Logic:
    1. Fetch full coin list with platform info (to get ETH contract addresses).
    2. Fetch Top N coins by Market Cap.
    3. Intersect: Keep Top N coins that are on Ethereum network.
    """
    # 1. Get Coin List with Platform info
    logger.info("Fetching full coin list with platform info...")
    all_coins_with_platform = await cg.get_coin_list(include_platform=True)
    
    if not all_coins_with_platform:
        logger.error("Failed to fetch coin list.")
        return []
        
    # Map coin_id -> platform info
    coin_platform_map = {
        c["id"]: c.get("platforms", {}).get("ethereum") 
        for c in all_coins_with_platform 
        if c.get("platforms", {}).get("ethereum")
    }
    
    logger.info(f"Found {len(coin_platform_map)} tokens on Ethereum in total.")

    # 2. Fetch Top N Coins by Market Cap
    logger.info(f"Fetching Top {top_n} coins by Market Cap...")
    # Fetch 3 pages of 100 to cover 250 safely
    top_coins_market = []
    for page in range(1, 4):
        markets = await cg.get_coin_markets(
            vs_currency="usd", 
            order="market_cap_desc", 
            per_page=100, 
            page=page
        )
        if markets:
            top_coins_market.extend(markets)
        else:
            break
            
    # Limit to requested top_n
    top_coins_market = top_coins_market[:top_n]
    
    # 3. Filter & Merge
    target_coins = []
    for coin in top_coins_market:
        c_id = coin["id"]
        # Check if this top coin is on Ethereum
        eth_address = coin_platform_map.get(c_id)
        
        # Note: Some major coins like ETH itself might not list 'ethereum' in platforms in the same way,
        # or wrapped versions. For native ETH, we handle it explicitly or ensure it's captured.
        # Coingecko ID for Ethereum is 'ethereum'.
        if c_id == 'ethereum':
            target_coins.append({
                "id": c_id,
                "symbol": coin["symbol"],
                "name": coin["name"],
                "eth_contract_address": None # Native coin
            })
        elif eth_address:
            target_coins.append({
                "id": c_id,
                "symbol": coin["symbol"],
                "name": coin["name"],
                "eth_contract_address": eth_address
            })
    
    logger.info(f"Identified {len(target_coins)} target Ethereum-compatible coins from Top {top_n} Market Cap.")
    return target_coins

def _safe_get_float(data: Dict[str, Any], key: str) -> float | None:
    """Safely extract float value from dict, returning None on failure."""
    val = data.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

async def ingest_market_data(kafka_output: str):
    """
    Ingests current market data for Ethereum tokens from CoinGecko.
    """
    producer = KafkaProducerWrapper(kafka_broker_url=kafka_output)
    
    async with CoinGeckoClient() as cg:
        # Fetch Top 250 coins on Ethereum
        target_coins = await _get_target_eth_coins(cg, top_n=250)
        if not target_coins:
            logger.warning("No target coins found.")
            return

        target_ids = [c["id"] for c in target_coins]
        
        # Fetch coin markets (Real-time data)
        # Note: get_all_coin_markets chunks requests automatically
        logger.info(f"Fetching market data for {len(target_ids)} tokens...")
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
                    "current_price": _safe_get_float(market_info, "current_price"),
                    "market_cap": _safe_get_float(market_info, "market_cap"),
                    "market_cap_rank": market_info.get("market_cap_rank"), # Int
                    "fully_diluted_valuation": _safe_get_float(market_info, "fully_diluted_valuation"),
                    "total_volume": _safe_get_float(market_info, "total_volume"),
                    "high_24h": _safe_get_float(market_info, "high_24h"),
                    "low_24h": _safe_get_float(market_info, "low_24h"),
                    "price_change_24h": _safe_get_float(market_info, "price_change_24h"),
                    "price_change_percentage_24h": _safe_get_float(market_info, "price_change_percentage_24h"),
                    "price_change_percentage_1h": _safe_get_float(market_info, "price_change_percentage_1h_in_currency"),
                    "price_change_percentage_7d": _safe_get_float(market_info, "price_change_percentage_7d_in_currency"),
                    "market_cap_change_24h": _safe_get_float(market_info, "market_cap_change_24h"),
                    "market_cap_change_percentage_24h": _safe_get_float(market_info, "market_cap_change_percentage_24h"),
                    "circulating_supply": _safe_get_float(market_info, "circulating_supply"),
                    "total_supply": _safe_get_float(market_info, "total_supply"),
                    "max_supply": _safe_get_float(market_info, "max_supply"),
                    "ath": _safe_get_float(market_info, "ath"),
                    "ath_change_percentage": _safe_get_float(market_info, "ath_change_percentage"),
                    "ath_date": market_info.get("ath_date"),
                    "atl": _safe_get_float(market_info, "atl"),
                    "atl_change_percentage": _safe_get_float(market_info, "atl_change_percentage"),
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
