from typing import List, Dict, Any, Tuple
from ingestion.ethereumetl.mappers.block_mapper import EthBlockMapper
from ingestion.ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ingestion.ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ingestion.ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.transaction import EthTransaction
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer
from utils.formatter_utils import hex_to_dec
from utils.logger_utils import get_logger

logger = get_logger("Eth Data Enricher")

class EthDataEnricher(object):
    """
    Encapsulates logic to transform raw RPC data (Blocks + Receipts) 
    into enriched domain objects (Blocks, Transactions, Receipts, TokenTransfers).
    
    Refactored to extract TokenTransfers directly from Receipts (Method 1) 
    for better reliability and data integrity.
    """
    def __init__(self):
        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        self.receipt_mapper = EthReceiptMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()

    def enrich_batch(
        self,
        raw_data: List[Dict],
        start_index: int,
        end_index: int
    ) -> Tuple[List[EthBlock], List[EthTransaction], List[EthReceipt], List[EthTokenTransfer], List[str]]:
        """
        Processes a batch of raw RPC responses (combined blocks and receipts).
        Expected raw_data structure: [Block_1, Block_2, ..., Receipt_1, Receipt_2, ...]
        """
        num_blocks = end_index - start_index + 1
        
        # Validate response length
        if len(raw_data) != num_blocks * 2:
            logger.error(f"Mismatch data length. Expected {num_blocks*2}, got {len(raw_data)}")
            return [], [], [], [], []

        block_responses = raw_data[:num_blocks]
        receipt_responses = raw_data[num_blocks:]

        blocks = []
        transactions = []
        receipts = []
        token_transfers = []
        contract_addresses = []

        for i in range(num_blocks):
            b_res = block_responses[i]
            r_res = receipt_responses[i]

            # 1. Process Block
            block_obj = self._process_block(b_res)
            if not block_obj:
                continue
            blocks.append(block_obj)

            # 2. Process Receipts & Extract Transfers (Method 1: Direct Extraction)
            # Now passing block_obj to enrich transfers with block context immediately
            receipt_map, batch_receipts, batch_transfers, batch_contract_addresses = self._process_receipts(r_res, block_obj)
            receipts.extend(batch_receipts)
            token_transfers.extend(batch_transfers)
            
            # 3. Process & Enrich Transactions (Only mapping tx details now)
            batch_txs = self._enrich_transactions(block_obj, b_res.get("result", {}), receipt_map)
            transactions.extend(batch_txs)
            
            # 4. Collect Contract Addresses
            contract_addresses.extend(batch_contract_addresses)

        return blocks, transactions, receipts, token_transfers, contract_addresses

    def _process_block(self, b_res: Dict) -> EthBlock | Any:
        if "error" in b_res or "result" not in b_res or b_res["result"] is None:
            logger.error(f"RPC Error or missing block. Full Response: {b_res}")
            return None
        
        try:
            return self.block_mapper.json_dict_to_block(b_res["result"])
        except Exception as e:
            logger.error(f"Block Mapping Error: {e}")
            return None

    def _process_receipts(self, r_res: Dict, block_obj: EthBlock) -> Tuple[Dict[str, Any], List[EthReceipt], List[EthTokenTransfer], List[str]]:
        receipt_map = {}
        receipts = []
        transfers = [] 
        contract_addresses = []
        
        if "result" in r_res and r_res["result"]:
            for r in r_res["result"]:
                # Normalization: Lowercase hash for reliable lookup
                if "transactionHash" in r and r["transactionHash"]:
                    receipt_map[r["transactionHash"].lower()] = r
                
                try:
                    # 1. Map Receipt
                    receipt_obj = self.receipt_mapper.json_dict_to_receipt(r)
                    receipts.append(receipt_obj)
                    
                    # 2. Capture contract address (Contract Creation)
                    if r.get("contractAddress"):
                        contract_addresses.append(r["contractAddress"])
                    
                    # 3. Extract Logs (Transfers & Interactions) - MOVED HERE
                    logs = r.get("logs", [])
                    for log in logs:
                        # 3a. Capture Contract Address from Log (Interaction)
                        contract_addr = log.get("address")
                        if contract_addr:
                            contract_addresses.append(contract_addr)
                        
                        # 3b. Extract Token Transfers
                        try:
                            transfers_list = self.token_transfer_mapper.json_dict_to_token_transfers(log, chain_id=block_obj.chain_id)
                            for transfer in transfers_list:
                                # Enrich with Block Context explicitly to ensure data integrity
                                transfer.block_number = block_obj.number
                                transfer.block_hash = block_obj.hash
                                transfer.block_timestamp = block_obj.timestamp
                                # Ensure tx hash is present (from receipt wrapper if missing in log)
                                if not transfer.transaction_hash:
                                    transfer.transaction_hash = r.get("transactionHash")
                                    
                                transfers.append(transfer)
                        except Exception as e:
                             logger.debug(f"Transfer Mapping Error: {e}")
                             pass

                except Exception as e:
                    logger.error(f"Receipt Processing Error: {e}")
        
        return receipt_map, receipts, transfers, contract_addresses

    def _enrich_transactions(
        self,
        block_obj: EthBlock,
        block_raw: Dict,
        receipt_map: Dict
    ) -> List[EthTransaction]:
        transactions = []

        if "transactions" in block_raw:
            for tx_raw in block_raw["transactions"]:
                if not isinstance(tx_raw, dict): continue

                try:
                    tx_obj = self.transaction_mapper.json_dict_to_transaction(tx_raw, block_timestamp=block_obj.timestamp)
                except Exception as e:
                    logger.error(f"Tx Mapping Error: {e}")
                    continue

                # Enrich with Receipt (Gas, Status)
                # Lookup with lowercase hash
                r_raw = receipt_map.get(tx_obj.hash.lower())
                if r_raw:
                    self._apply_receipt_to_tx(tx_obj, r_raw)
                
                # Note: No longer extracting logs/transfers here.
                # Logic moved to _process_receipts.

                transactions.append(tx_obj)
        
        return transactions

    @staticmethod
    def _apply_receipt_to_tx(tx_obj: EthTransaction, r_raw: Dict):
        tx_obj.receipt_cumulative_gas_used = hex_to_dec(r_raw.get("CumulativeGasUsed"))
        eff_gas = r_raw.get("effectiveGasPrice")
        if eff_gas:
            tx_obj.receipt_effective_gas_price = hex_to_dec(eff_gas)

        tx_obj.receipt_gas_used = hex_to_dec(r_raw.get("gasUsed"))
        tx_obj.receipt_blob_gas_price = hex_to_dec(r_raw.get("BlobGasPrice"))
        tx_obj.receipt_blob_gas_used = hex_to_dec(r_raw.get("blobGasUsed"))
        tx_obj.receipt_contract_address = r_raw.get("contractAddress")
        tx_obj.receipt_status = hex_to_dec(r_raw.get("status"))
        tx_obj.receipt_root = hex_to_dec(r_raw.get("root"))