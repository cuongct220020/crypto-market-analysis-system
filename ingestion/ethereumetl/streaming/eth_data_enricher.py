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
    ) -> Tuple[List[EthBlock], List[EthTransaction], List[EthReceipt], List[EthTokenTransfer]]:
        """
        Processes a batch of raw RPC responses (combined blocks and receipts).
        Expected raw_data structure: [Block_1, Block_2, ..., Receipt_1, Receipt_2, ...]
        """
        num_blocks = end_index - start_index + 1
        
        # Validate response length
        if len(raw_data) != num_blocks * 2:
            logger.error(f"Mismatch data length. Expected {num_blocks*2}, got {len(raw_data)}")
            return [], [], [], []

        block_responses = raw_data[:num_blocks]
        receipt_responses = raw_data[num_blocks:]

        blocks = []
        transactions = []
        receipts = []
        token_transfers = []

        for i in range(num_blocks):
            b_res = block_responses[i]
            r_res = receipt_responses[i]

            # 1. Process Block
            block_obj = self._process_block(b_res)
            if not block_obj:
                continue
            blocks.append(block_obj)

            # 2. Process Receipts
            receipt_map, batch_receipts, batch_transfers_from_receipts = self._process_receipts(r_res)
            receipts.extend(batch_receipts)
            
            # Note: We get transfers from receipts logic if we parse logs there. 
            # Current logic in IngestionWorker extracted transfers during TX enrichment from receipts map.
            # We can do it in _process_receipts or _enrich_transactions.
            # Ideally, TokenTransfers are derived from Receipts (Logs).
            
            # 3. Process & Enrich Transactions
            batch_txs, batch_transfers = self._enrich_transactions(block_obj, b_res.get("result", {}), receipt_map)
            transactions.extend(batch_txs)
            token_transfers.extend(batch_transfers)

        return blocks, transactions, receipts, token_transfers

    def _process_block(self, b_res: Dict) -> Any:
        if "error" in b_res or "result" not in b_res or b_res["result"] is None:
            logger.error(f"RPC Error or missing block. Full Response: {b_res}")
            return None
        
        try:
            return self.block_mapper.json_dict_to_block(b_res["result"])
        except Exception as e:
            logger.error(f"Block Mapping Error: {e}")
            return None

    def _process_receipts(self, r_res: Dict) -> Tuple[Dict[str, Any], List[EthReceipt], List[EthTokenTransfer]]:
        receipt_map = {}
        receipts = []
        transfers = [] # If we wanted to extract here
        
        if "result" in r_res and r_res["result"]:
            for r in r_res["result"]:
                receipt_map[r["transactionHash"]] = r
                try:
                    receipt_obj = self.receipt_mapper.json_dict_to_receipt(r)
                    receipts.append(receipt_obj)
                except Exception as e:
                    logger.error(f"Receipt Mapping Error: {e}")
        
        return receipt_map, receipts, transfers

    def _enrich_transactions(
        self,
        block_obj: EthBlock,
        block_raw: Dict,
        receipt_map: Dict
    ) -> Tuple[List[EthTransaction], List[EthTokenTransfer]]:
        transactions = []
        token_transfers = []

        if "transactions" in block_raw:
            for tx_raw in block_raw["transactions"]:
                if not isinstance(tx_raw, dict): continue

                try:
                    tx_obj = self.transaction_mapper.json_dict_to_transaction(tx_raw, block_timestamp=block_obj.timestamp)
                except Exception as e:
                    logger.error(f"Tx Mapping Error: {e}")
                    continue

                # Enrich with Receipt
                r_raw = receipt_map.get(tx_obj.hash)
                if r_raw:
                    self._apply_receipt_to_tx(tx_obj, r_raw)
                    
                    # Extract Transfers from Logs
                    logs = r_raw.get("logs", [])
                    for log in logs:
                        try:
                            # Using the mapper which works on dicts (raw logs)
                            transfer = self.token_transfer_mapper.json_dict_to_token_transfer(log)
                            if transfer:
                                transfer.block_number = block_obj.number
                                transfer.block_hash = block_obj.hash
                                transfer.transaction_hash = tx_obj.hash
                                transfer.block_timestamp = block_obj.timestamp
                                token_transfers.append(transfer)
                        except Exception as e:
                             logger.debug(f"Transfer Mapping Error: {e}")
                             pass

                transactions.append(tx_obj)
        
        return transactions, token_transfers

    @staticmethod
    def _apply_receipt_to_tx(tx_obj: EthTransaction, r_raw: Dict):
        tx_obj.receipt_gas_used = hex_to_dec(r_raw.get("gasUsed"))
        tx_obj.receipt_status = hex_to_dec(r_raw.get("status"))
        
        eff_gas = r_raw.get("effectiveGasPrice")
        if eff_gas:
            tx_obj.receipt_effective_gas_price = hex_to_dec(eff_gas)
            
        tx_obj.receipt_contract_address = r_raw.get("contractAddress")
