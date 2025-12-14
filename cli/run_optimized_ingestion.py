import multiprocessing
import os
import time
import argparse
from typing import Optional

from ingestion.optimized.worker import worker_entrypoint
from utils.logger_utils import get_logger

logger = get_logger("OptimizedIngestionOrchestrator")

# Default Chunks of work to put in the Queue
# Each worker picks one chunk, does (end-start)/batch_size RPC calls.
CHUNK_SIZE = 100 

def run_ingestion(
    start_block: int,
    end_block: int,
    rpc_url: str,
    kafka_broker_url: str,
    num_workers: int,
    rate_sleep: float
):
    """
    Orchestrates the parallel ingestion process using Work Stealing pattern.
    """
    logger.info(f"Starting Ingestion: {start_block}-{end_block}")
    logger.info(f"Workers: {num_workers} | Rate Sleep: {rate_sleep}s")
    
    manager = multiprocessing.Manager()
    job_queue = manager.Queue()
    
    # 1. Populate Queue (Dynamic Work Stealing)
    # We chop the total range into small chunks (e.g., 100 blocks).
    # Workers compete to grab these chunks.
    total_chunks = 0
    for i in range(start_block, end_block + 1, CHUNK_SIZE):
        chunk_end = min(i + CHUNK_SIZE - 1, end_block)
        job_queue.put((i, chunk_end))
        total_chunks += 1
        
    logger.info(f"Generated {total_chunks} work chunks.")
    
    # Add Sentinels (Poison Pill) to signal workers to exit
    for _ in range(num_workers):
        job_queue.put(None)
        
    # 2. Spawn Workers
    workers = []
    for i in range(num_workers):
        p = multiprocessing.Process(
            target=worker_entrypoint,
            args=(i, rpc_url, kafka_broker_url, job_queue, rate_sleep)
        )
        p.start()
        workers.append(p)
        
    # 3. Wait for completion
    start_time = time.time()
    
    # Simple monitoring
    for p in workers:
        p.join()
        
    total_time = time.time() - start_time
    total_blocks = end_block - start_block + 1
    speed = total_blocks / total_time if total_time > 0 else 0
    
    logger.info(f"Ingestion Completed in {total_time:.2f}s.")
    logger.info(f"Average Speed: {speed:.2f} blocks/s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimized Ethereum ETL Ingestion")
    parser.add_argument("-s", "--start-block", type=int, required=True, help="Start Block Number")
    parser.add_argument("-e", "--end-block", type=int, required=True, help="End Block Number")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of Worker Processes")
    parser.add_argument("-r", "--rpc-url", type=str, required=True, help="Ethereum RPC URL")
    parser.add_argument("-k", "--kafka-url", type=str, default="localhost:9092", help="Kafka Broker URL")
    parser.add_argument("--rate-sleep", type=float, default=0.5, help="Sleep time between batch requests (seconds)")
    
    args = parser.parse_args()
    
    run_ingestion(
        start_block=args.start_block,
        end_block=args.end_block,
        rpc_url=args.rpc_url,
        kafka_broker_url=args.kafka_url,
        num_workers=args.workers,
        rate_sleep=args.rate_sleep
    )
