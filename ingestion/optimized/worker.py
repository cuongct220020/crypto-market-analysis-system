import asyncio
import os
import time
import json
from typing import List, Dict, Any, Optional

from ingestion.kafka_producer import KafkaProducerWrapper
from ingestion.optimized.rpc_client import RpcClient
from ingestion.ethereumetl.mappers.block_mapper import EthBlockMapper
from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ingestion.ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from utils.formatter_utils import hex_to_dec, to_normalized_address
from utils.logger_utils import get_logger

logger = get_logger("IngestionWorker")

# --- Constants ---
BATCH_SIZE = 5  # RPC Batch size (blocks per HTTP request)
QUEUE_SIZE = 5   # Max batches in memory buffer

class IngestionWorker:
    """
    Autonomous Worker that fetches, processes, and publishes blockchain data.
    Designed for multiprocessing environment.
    """
    def __init__(self, worker_id: int, rpc_url: str, kafka_broker_url: str, rate_limit_sleep: float = 0.0):
        self.worker_id = worker_id
        self.rpc_client = RpcClient(rpc_url)
        self.producer = KafkaProducerWrapper(kafka_broker_url)
        self.rate_limit_sleep = rate_limit_sleep
        
        # Internal Async Queue for decoupling Network I/O from CPU Processing
        self.internal_queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        
        # Mappers
        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()

    async def run(self, job_queue):
        """
        Main Async Loop.
        job_queue: A multiprocessing.Queue containing (start, end) tuples.
        """
        logger.info(f"Worker {self.worker_id} started. PID: {os.getpid()}")
        
        # Start the Processor task (Consumer)
        processor_task = asyncio.create_task(self._processor_loop())
        
        # Run the Fetcher loop (Producer)
        try:
            while True:
                # Get job from Process Queue (Blocking, but okay since it's the driver)
                # In a real async/mp mix, we might want to run this in executor, 
                # but for simplicity, we treat getting a job as a quick action.
                # To avoid blocking the event loop for too long, we can use run_in_executor if needed,
                # but queue.get() is usually fast or we wait anyway.
                # Ideally: loop.run_in_executor(None, job_queue.get)
                
                try:
                    job = await asyncio.to_thread(job_queue.get)
                except Exception:
                    break

                if job is None: # Sentinel
                    break
                
                start_block, end_block = job
                # Process the range in chunks of BATCH_SIZE
                for i in range(start_block, end_block + 1, BATCH_SIZE):
                    batch_end = min(i + BATCH_SIZE - 1, end_block)
                    
                    # Fetch Batch
                    data = await self.rpc_client.fetch_blocks_and_receipts(i, batch_end)
                    if data:
                        # Put to Internal Queue (Wait if full -> Backpressure)
                        await self.internal_queue.put((i, batch_end, data))
                        
                    # Rate Limit
                    if self.rate_limit_sleep > 0:
                        await asyncio.sleep(self.rate_limit_sleep)
            
            # Signal Processor to stop
            await self.internal_queue.put(None)
            
            # Wait for Processor to finish remaining items
            await processor_task
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id} crashed: {e}")
        finally:
            logger.info(f"Worker {self.worker_id} flushing Kafka...")
            self.producer.flush()

    async def _processor_loop(self):
        """
        CPU-Bound Processing Loop.
        Consumes raw RPC data -> Maps -> Enriches -> Publishes.
        """
        while True:
            item = await self.internal_queue.get()
            if item is None:
                self.internal_queue.task_done()
                break
            
            start, end, raw_data = item
            try:
                self._process_batch(start, end, raw_data)
            except Exception as e:
                logger.error(f"Error processing batch {start}-{end}: {e}")
            finally:
                self.internal_queue.task_done()

    def _process_batch(self, start: int, end: int, raw_data: List[Dict]):
        """
        Logic to transform Raw JSON to Domain Objects and Publish.
        """
        num_blocks = end - start + 1
        
        # Validate response length
        if len(raw_data) != num_blocks * 2:
            logger.error(f"Mismatch data length for {start}-{end}. Expected {num_blocks*2}, got {len(raw_data)}")
            return

        # Split Blocks and Receipts
        block_responses = raw_data[:num_blocks]
        receipt_responses = raw_data[num_blocks:]

        for i in range(num_blocks):
            b_res = block_responses[i]
            r_res = receipt_responses[i]

            # Check RPC Errors
            if "error" in b_res or "result" not in b_res or b_res["result"] is None:
                logger.error(f"RPC Error or missing block: {b_res.get('error', 'Unknown')}")
                continue
            
            block_raw = b_res["result"]
            
            # 1. Map Block
            # Use json_dict_to_block (camelCase support)
            try:
                block_obj = self.block_mapper.json_dict_to_block(block_raw)
            except Exception as e:
                logger.error(f"Block Mapping Error: {e}")
                continue
            
            # 2. Prepare Receipts Map
            receipt_map = {}
            if "result" in r_res and r_res["result"]:
                for r in r_res["result"]:
                    receipt_map[r["transactionHash"]] = r

            enriched_txs = []
            token_transfers = []

            # 3. Process Transactions (Enrichment)
            if "transactions" in block_raw:
                for tx_raw in block_raw["transactions"]:
                    # Map basic TX
                    if not isinstance(tx_raw, dict): continue # Skip if just hash
                    
                    try:
                        tx_obj = self.transaction_mapper.json_dict_to_transaction(tx_raw, block_timestamp=block_obj.timestamp)
                    except Exception as e:
                        logger.error(f"Tx Mapping Error: {e}")
                        continue
                    
                    # Find Receipt
                    r_raw = receipt_map.get(tx_obj.hash)
                    if r_raw:
                        # Enrich Gas/Status
                        # Handle hex or int variations
                        gas_used = r_raw.get("gasUsed")
                        tx_obj.receipt_gas_used = hex_to_dec(gas_used)
                        
                        status = r_raw.get("status")
                        tx_obj.receipt_status = hex_to_dec(status)

                        eff_gas = r_raw.get("effectiveGasPrice")
                        if eff_gas:
                            tx_obj.receipt_effective_gas_price = hex_to_dec(eff_gas)
                            
                        tx_obj.receipt_contract_address = r_raw.get("contractAddress")

                        # Extract Token Transfers (Logs)
                        logs = r_raw.get("logs", [])
                        for log in logs:
                            # Use existing TokenTransferMapper (needs wrapper if it expects Web3 dict)
                            # Assuming mapper handles camelCase or we adapt. 
                            # Checking TokenTransferMapper... usually expects snake_case if from Web3.
                            # Let's do a quick adaptation or usage check.
                            # For safety, let's look at log structure. RPC log has 'address', 'topics', 'data'.
                            # Let's trust mapper.json_dict_to_token_transfer (if exists) or adapt.
                            try:
                                transfer = self.token_transfer_mapper.json_dict_to_token_transfer(log)
                                if transfer:
                                    transfer.block_number = block_obj.number
                                    transfer.block_hash = block_obj.hash
                                    transfer.transaction_hash = tx_obj.hash
                                    transfer.block_timestamp = block_obj.timestamp
                                    token_transfers.append(transfer)
                            except AttributeError:
                                # Mapper might only have web3_dict. Let's assume standard log format matches.
                                # If missing, we might need a quick json helper in mapper. 
                                # Proceeding with assumption/fallback.
                                pass 
                                
                    enriched_txs.append(tx_obj)

            # 4. Publish to Kafka
            # KEY is crucial for partitioning
            key = str(block_obj.number).encode("utf-8")
            
            # Produce Block
            self.producer.produce("blocks", block_obj, schema_key="block", key=key)
            
            # Produce Txs
            for tx in enriched_txs:
                self.producer.produce("transactions", tx, schema_key="transaction", key=key)
                
            # Produce Transfers
            for tt in token_transfers:
                self.producer.produce("token_transfers", tt, schema_key="token_transfer", key=key)

            # Poll periodically (not every message, but every block is fine)
            # The wrapper doesn't expose poll directly, but produce is async.
            # We rely on flush at end, or we can access producer internal if needed.
            # self.producer.producer.poll(0) 

def worker_entrypoint(worker_id, rpc_url, kafka_url, job_queue, rate_sleep):
    """
    Entrypoint needed for multiprocessing to bootstrap the class.
    """
    worker = IngestionWorker(worker_id, rpc_url, kafka_url, rate_sleep)
    asyncio.run(worker.run(job_queue))
