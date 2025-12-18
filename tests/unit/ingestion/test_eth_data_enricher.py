import pytest
from ingestion.ethereumetl.streaming.eth_data_enricher import EthDataEnricher
from ingestion.ethereumetl.models.block import EthBlock
from ingestion.ethereumetl.models.transaction import EthTransaction
from ingestion.ethereumetl.models.receipt import EthReceipt
from ingestion.ethereumetl.models.token_transfer import EthTokenTransfer

# Sample Raw Data
RAW_BLOCK = {
    "jsonrpc": "2.0",
    "result": {
        "number": "0x1",
        "hash": "0xabc",
        "timestamp": "0x656565",
        "transactions": [
            {
                "hash": "0xtx1",
                "from": "0xsender",
                "to": "0xreceiver",
                "value": "0x100"
            }
        ]
    },
    "id": 1
}

RAW_RECEIPT = {
    "jsonrpc": "2.0",
    "result": [
        {
            "transactionHash": "0xtx1",
            "status": "0x1",
            "gasUsed": "0x5208",
            "logs": [
                {
                    "address": "0xtoken",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", # Transfer
                        "0x000000000000000000000000sender00000000000000000000000000000000",
                        "0x000000000000000000000000receiver00000000000000000000000000000000"
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000000000000000064", # 100
                    "transactionHash": "0xtx1",
                    "blockNumber": "0x1",
                    "logIndex": "0x0"
                }
            ]
        }
    ],
    "id": 2
}

def test_enrich_batch():
    enricher = EthDataEnricher()

    # Simulate a batch with 1 block and 1 receipt response
    raw_data = [RAW_BLOCK, RAW_RECEIPT]

    blocks, txs, receipts, transfers, contract_addresses = enricher.enrich_batch(raw_data, 1, 1)

    # Assert Blocks
    assert len(blocks) == 1
    assert isinstance(blocks[0], EthBlock)
    assert blocks[0].number == 1

    # Assert Transactions (Enriched)
    assert len(txs) == 1
    assert isinstance(txs[0], EthTransaction)
    assert txs[0].hash == "0xtx1"
    assert txs[0].receipt_status == 1 # Enriched from receipt

    # Assert Receipts
    assert len(receipts) == 1
    assert isinstance(receipts[0], EthReceipt)
    assert receipts[0].transaction_hash == "0xtx1"

    # Assert Token Transfers (Extracted from Logs)
    assert len(transfers) == 1
    assert isinstance(transfers[0], EthTokenTransfer)
    assert transfers[0].contract_address == "0xtoken"
    assert transfers[0].amounts[0].value == "100"
    assert transfers[0].token_standard == "ERC20"

    # Assert Contract Addresses
    assert isinstance(contract_addresses, list)

def test_enrich_batch_with_rpc_error():
    enricher = EthDataEnricher()

    raw_block_error = {"jsonrpc": "2.0", "error": {"code": -32000, "message": "Execution error"}, "id": 1}
    raw_data = [raw_block_error, RAW_RECEIPT]

    blocks, txs, receipts, transfers, contract_addresses = enricher.enrich_batch(raw_data, 1, 1)

    # Should handle gracefully
    assert len(blocks) == 0
    assert len(txs) == 0
    # Receipts might still be processed if their response is valid, but they won't link to a block in this batch
    # Current logic processes blocks then receipts. If block fails, loop continues?
    # In Enricher logic:
    # block_obj = self._process_block(b_res)
    # if not block_obj: continue
    # So if block fails, we skip receipts and tx processing for that index.
    assert len(receipts) == 0
    assert len(transfers) == 0
    assert len(contract_addresses) == 0
