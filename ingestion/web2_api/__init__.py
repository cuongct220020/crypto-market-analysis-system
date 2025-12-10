from datetime import datetime
from typing import Any, Dict, List
import asyncio

from ingestion.web2_api.coin_gecko import CoinGeckoAPI
from ingestion.web2_api.coin_market_cap import CoinMarketCapAPI
from ingestion.web2_api.defillama import DefiLlamaAPI
from ingestion.web2_api.chain_link import ChainlinkAPI
from config.settings import settings
from utils.logger_utils import get_logger

logger = get_logger("Crypto Data Collector")

class CryptoDataCollector:
    """
    A service class to collect cryptocurrency data from multiple APIs
    and push to Kafka for the streaming pipeline.
    """
    
    def __init__(self):
        self.coin_gecko_api = CoinGeckoAPI()
        self.coin_market_cap_api = CoinMarketCapAPI()
        self.defillama_api = DefiLlamaAPI()
        self.chainlink_api = ChainlinkAPI()
    
    async def collect_top_coins_data(self) -> Dict[str, Any]:
        """
        Collect top coins data from multiple sources
        """
        logger.info("Starting collection of top coins data...")
        
        collected_data = {
            "timestamp": datetime.now().isoformat(),
            "source": "multi_source",
            "data": {}
        }
        
        try:
            # Collect from CoinGecko
            async with self.coin_gecko_api as api:
                logger.info("Collecting top coins from CoinGecko...")
                top_coins_cg = await api.get_coin_markets(vs_currency="usd", limit=100)
                collected_data["data"]["coingecko"] = {
                    "top_coins": top_coins_cg,
                    "collected_at": datetime.now().isoformat()
                }
                logger.info(f"Collected {len(top_coins_cg)} top coins from CoinGecko")
        
        except Exception as e:
            logger.error(f"Error collecting data from CoinGecko: {str(e)}")
        
        try:
            # Collect from CoinMarketCap
            async with self.coin_market_cap_api as api:
                logger.info("Collecting top coins from CoinMarketCap...")
                top_coins_cmc = await api.get_listings_latest(limit=100)
                collected_data["data"]["coinmarketcap"] = {
                    "top_coins": top_coins_cmc.get("data", []),
                    "collected_at": datetime.now().isoformat()
                }
                logger.info(f"Collected {len(top_coins_cmc.get('data', []))} top coins from CoinMarketCap")
        
        except Exception as e:
            logger.error(f"Error collecting data from CoinMarketCap: {str(e)}")
        
        return collected_data
    
    async def collect_protocol_data(self) -> Dict[str, Any]:
        """
        Collect DeFi protocol data
        """
        logger.info("Starting collection of DeFi protocol data...")
        
        collected_data = {
            "timestamp": datetime.now().isoformat(),
            "source": "defillama",
            "data": {}
        }
        
        try:
            # Collect from DefiLlama
            async with self.defillama_api as api:
                logger.info("Collecting DeFi protocols from DefiLlama...")
                protocols = await api.get_protocols()
                collected_data["data"]["defillama"] = {
                    "protocols": protocols,
                    "collected_at": datetime.now().isoformat()
                }
                logger.info(f"Collected {len(protocols)} DeFi protocols from DefiLlama")
        
        except Exception as e:
            logger.error(f"Error collecting data from DefiLlama: {str(e)}")
        
        return collected_data
    
    async def collect_real_time_prices(self) -> Dict[str, Any]:
        """
        Collect real-time prices using Chainlink
        """
        logger.info("Starting collection of real-time prices...")
        
        collected_data = {
            "timestamp": datetime.now().isoformat(),
            "source": "chainlink",
            "data": {}
        }
        
        try:
            # Collect from Chainlink
            async with self.chainlink_api as api:
                logger.info("Collecting real-time prices from Chainlink...")
                # For example, get ETH/USD price feed
                eth_price = await api.get_aggregator_prices("eth-usd")
                collected_data["data"]["chainlink"] = {
                    "eth_usd_price": eth_price,
                    "collected_at": datetime.now().isoformat()
                }
                logger.info("Collected real-time prices from Chainlink")
        
        except Exception as e:
            logger.error(f"Error collecting data from Chainlink: {str(e)}")
        
        return collected_data
    
    async def collect_all_data(self) -> Dict[str, Any]:
        """
        Collect all types of data in parallel
        """
        logger.info("Starting comprehensive data collection...")
        
        # Run all collection methods in parallel
        results = await asyncio.gather(
            self.collect_top_coins_data(),
            self.collect_protocol_data(), 
            self.collect_real_time_prices(),
            return_exceptions=True
        )
        
        all_collected_data = {}
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error in collection task {i}: {str(result)}")
                continue
            
            all_collected_data.update(result)
        
        logger.info("Completed comprehensive data collection")
        return all_collected_data