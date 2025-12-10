import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from hexbytes import HexBytes
from ingestion.ethereumetl.jobs.export_receipts_job import ExportReceiptsJob

@pytest.fixture
def mock_web3():
    web3 = MagicMock()
    web3.eth = MagicMock()
    web3.eth.get_transaction_receipt = AsyncMock()
    return web3

@pytest.fixture
def mock_item_exporter():
    return MagicMock()

@pytest.fixture
def mock_batch_work_executor():
    with patch("ingestion.ethereumetl.jobs.export_receipts_job.AsyncBatchWorkExecutor") as MockExecutor:
        mock_instance = MockExecutor.return_value
        mock_instance.execute = AsyncMock()
        mock_instance.shutdown = MagicMock()
        yield mock_instance

def test_initialization_validation(mock_web3, mock_item_exporter):
    with pytest.raises(ValueError, match="At least one of export_receipts or export_logs must be True"):
        ExportReceiptsJob(
            transaction_hashes_iterable=[],
            batch_size=1,
            web3=mock_web3,
            max_workers=1,
            item_exporter=mock_item_exporter,
            export_receipts=False,
            export_logs=False
        )

@pytest.mark.asyncio
async def test_export_batch(mock_web3, mock_item_exporter):
    job = ExportReceiptsJob(
        transaction_hashes_iterable=["0x123", "0x456"],
        batch_size=2,
        web3=mock_web3,
        max_workers=1,
        item_exporter=mock_item_exporter
    )
    
    mock_receipt_data = {'transactionHash': HexBytes('0x123')}
    
    # Mock gather_with_concurrency
    with patch("ingestion.ethereumetl.jobs.export_receipts_job.gather_with_concurrency", new_callable=AsyncMock) as mock_gather:
        mock_gather.return_value = [mock_receipt_data, None] # One valid, one invalid/None
        
        # Mock mappers
        with patch("ingestion.ethereumetl.jobs.export_receipts_job.EthReceiptMapper") as MockReceiptMapper:
            mock_mapper = MockReceiptMapper.return_value
            mock_receipt_model = MagicMock()
            mock_receipt_model.logs = []
            mock_mapper.web3_dict_to_receipt.return_value = mock_receipt_model
            
            job.receipt_mapper = mock_mapper
            
            await job._export_batch(["0x123", "0x456"])
            
            # Check mapper called for valid receipt
            mock_mapper.web3_dict_to_receipt.assert_called_once_with(mock_receipt_data)
            
            # Check export item called
            mock_item_exporter.export_item.assert_called_with(mock_receipt_model)

@pytest.mark.asyncio
async def test_export_batch_invalid_hash(mock_web3, mock_item_exporter):
    job = ExportReceiptsJob(
        transaction_hashes_iterable=["0xinvalid"],
        batch_size=1,
        web3=mock_web3,
        max_workers=1,
        item_exporter=mock_item_exporter
    )
    
    # Mock web3 raising exception
    mock_web3.eth.get_transaction_receipt.side_effect = Exception("Invalid Hash")
    
    with patch("ingestion.ethereumetl.jobs.export_receipts_job.gather_with_concurrency", new_callable=AsyncMock) as mock_gather:
        # If one fails, the code appends _async_return_none task. 
        # gather_with_concurrency should return [None]
        mock_gather.return_value = [None]
        
        await job._export_batch(["0xinvalid"])
        
        # Verify export_item NOT called
        mock_item_exporter.export_item.assert_not_called()

@pytest.mark.asyncio
async def test_export_logs(mock_web3, mock_item_exporter):
    job = ExportReceiptsJob(
        transaction_hashes_iterable=["0x123"],
        batch_size=1,
        web3=mock_web3,
        max_workers=1,
        item_exporter=mock_item_exporter,
        export_logs=True
    )
    
    with patch("ingestion.ethereumetl.jobs.export_receipts_job.gather_with_concurrency", new_callable=AsyncMock) as mock_gather:
        mock_gather.return_value = [{'transactionHash': HexBytes('0x123')}]
        
        with patch("ingestion.ethereumetl.jobs.export_receipts_job.EthReceiptMapper") as MockMapper:
            mock_mapper = MockMapper.return_value
            mock_receipt = MagicMock()
            mock_log = MagicMock()
            mock_receipt.logs = [mock_log]
            mock_mapper.web3_dict_to_receipt.return_value = mock_receipt
            
            job.receipt_mapper = mock_mapper
            
            await job._export_batch(["0x123"])
            
            # Should export receipt AND log
            assert mock_item_exporter.export_item.call_count == 2
