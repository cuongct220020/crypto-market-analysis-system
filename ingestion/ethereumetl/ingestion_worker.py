import asyncio
import os
from typing import List, Dict

from ingestion.kafka_producer_wrapper import KafkaProducerWrapper
from ingestion.ethereumetl.rpc_client import RpcClient
from ingestion.ethereumetl.streaming.eth_data_enricher import EthDataEnricher
from utils.logger_utils import get_logger

logger = get_logger("Ingestion Worker")


class IngestionWorker(object):
    """
    Autonomous Worker that fetches, processes, and publishes blockchain data.
    Designed for multiprocessing environment.
    """
    def __init__(self, worker_id: int,
                 rpc_url: str,
                 kafka_broker_url: str,
                 rpc_batch_request_size: int,
                 worker_internal_queue_size: int,
                 progress_queue=None,
                 rate_limit_sleep: float = 0.0):
        self._worker_id = worker_id
        self._rpc_client = RpcClient(rpc_url)
        self._producer = KafkaProducerWrapper(kafka_broker_url)
        self._rate_limit_sleep = rate_limit_sleep
        self._rpc_batch_request_size = rpc_batch_request_size
        self._worker_internal_queue_size = worker_internal_queue_size
        self.progress_queue = progress_queue
        
        # Internal Async Queue for decoupling Network I/O from CPU Processing
        self.internal_queue = asyncio.Queue(maxsize=self._worker_internal_queue_size)
        
        # Enricher deals with data transformation
        self.enricher = EthDataEnricher()

    async def run(self, job_queue):
        """
        Main Async Loop.
        job_queue: A multiprocessing.Queue containing (start, end) tuples.
        """
        logger.info(f"Worker {self._worker_id} started. PID: {os.getpid()}")
        
        # Start the Processor task (Consumer)
        processor_task = asyncio.create_task(self._processor_loop())
        
        # Run the Fetcher loop (Producer)
        try:
            while True:
                try:
                    job = await asyncio.to_thread(job_queue.get)
                except Exception as err:
                    raise RuntimeError(f"Error getting job: {err}")

                if job is None: # Sentinel
                    break
                
                start_block, end_block = job
                # Process the range in chunks of BATCH_SIZE
                for i in range(start_block, end_block + 1, self._rpc_batch_request_size):
                    batch_end = min(i + self._rpc_batch_request_size - 1, end_block)
                    
                    # Fetch Batch
                    data = await self._rpc_client.fetch_blocks_and_receipts(i, batch_end)
                    if data:
                        # Put to Internal Queue (Wait if full -> Backpressure)
                        await self.internal_queue.put((i, batch_end, data))
                        
                    # Rate Limit
                    if self._rate_limit_sleep > 0:
                        await asyncio.sleep(self._rate_limit_sleep)
            
            # Signal Processor to stop
            await self.internal_queue.put(None)
            
            # Wait for Processor to finish remaining items
            await processor_task
            
        except Exception as e:
            logger.error(f"Worker {self._worker_id} crashed: {e}")
        finally:
            logger.info(f"Worker {self._worker_id} shutting down rpc client...")
            await self._rpc_client.close()
            logger.info(f"Worker {self._worker_id} flushing Kafka...")
            self._producer.flush()

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
        # Delegate complex logic to Enricher
        blocks, transactions, receipts, token_transfers = self.enricher.enrich_batch(raw_data, start, end)

        # Publish to Kafka
        for block_obj in blocks:
            key = str(block_obj.number).encode("utf-8")
            self._producer.produce("blocks", block_obj, schema_key="block", key=key)

        # Note: We use the block number of the item as key for partitioning to keep data locality
        # We assume transactions/receipts/transfers have block_number set correctly by enricher.
        
        for tx in transactions:
            key = str(tx.block_number).encode("utf-8")
            self._producer.produce("transactions", tx, schema_key="transaction", key=key)

        for receipt in receipts:
            key = str(receipt.block_number).encode("utf-8")
            self._producer.produce("receipts", receipt, schema_key="receipt", key=key)

        for tt in token_transfers:
            key = str(tt.block_number).encode("utf-8")
            self._producer.produce("token_transfers", tt, schema_key="token_transfer", key=key)
            
        # Report Progress
        if self.progress_queue and blocks:
            try:
                self.progress_queue.put(len(blocks))
            except Exception:
                pass # Ignore queue errors (e.g., closed)

def worker_entrypoint(
        worker_id,
        rpc_url,
        kafka_url,
        job_queue,
        rpc_batch_request_size,
        worker_internal_queue_size,
        rate_limit_sleep,
        progress_queue=None
):
    """
    Entrypoint needed for multiprocessing to bootstrap the class.
    """
    worker = IngestionWorker(
        worker_id=worker_id,
        rpc_url=rpc_url,
        kafka_broker_url=kafka_url,
        rpc_batch_request_size=rpc_batch_request_size,
        worker_internal_queue_size=worker_internal_queue_size,
        rate_limit_sleep=rate_limit_sleep,
        progress_queue=progress_queue
    )
    asyncio.run(worker.run(job_queue))
