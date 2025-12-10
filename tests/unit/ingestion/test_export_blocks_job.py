import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from ingestion.ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ingestion.ethereumetl.models.block import EthBlock

@pytest.fixture
def mock_web3():
    web3 = MagicMock()
    web3.eth = MagicMock()
    web3.eth.get_block = AsyncMock()
    return web3

@pytest.fixture
def mock_item_exporter():
    return MagicMock()

@pytest.fixture
def mock_batch_work_executor():
    with patch("ingestion.ethereumetl.jobs.export_blocks_job.AsyncBatchWorkExecutor") as MockExecutor:
        mock_instance = MockExecutor.return_value
        mock_instance.execute_pipeline = AsyncMock()
        mock_instance.shutdown = MagicMock()
        yield mock_instance

def test_initialization_validation(mock_web3, mock_item_exporter):
    with pytest.raises(ValueError, match="range_start and range_end must be greater than or equal to 0"):
        ExportBlocksJob(start_block=-1, end_block=10, batch_size=1, web3=mock_web3, max_workers=1, item_exporter=mock_item_exporter)

    with pytest.raises(ValueError, match="range_end must be greater than or equal to range_start"):
        ExportBlocksJob(start_block=10, end_block=5, batch_size=1, web3=mock_web3, max_workers=1, item_exporter=mock_item_exporter)

    with pytest.raises(ValueError, match="At least one of export_blocks or export_transactions must be True"):
        ExportBlocksJob(start_block=0, end_block=10, batch_size=1, web3=mock_web3, max_workers=1, item_exporter=mock_item_exporter, export_blocks=False, export_transactions=False)

@pytest.mark.asyncio
async def test_export_pipeline_execution(mock_web3, mock_item_exporter, mock_batch_work_executor):
    job = ExportBlocksJob(
        start_block=0, 
        end_block=10, 
        batch_size=5, 
        web3=mock_web3, 
        max_workers=5, 
        item_exporter=mock_item_exporter
    )
    
    await job._start()
    mock_item_exporter.open.assert_called_once()
    
    await job._export()
    mock_batch_work_executor.execute_pipeline.assert_called_once()
    
    await job._end()
    mock_batch_work_executor.shutdown.assert_called_once()
    mock_item_exporter.close.assert_called_once()

@pytest.mark.asyncio
async def test_fetch_batch(mock_web3, mock_item_exporter):
    job = ExportBlocksJob(
        start_block=0, end_block=10, batch_size=5, web3=mock_web3, max_workers=5, item_exporter=mock_item_exporter
    )
    
    # Mock gather_with_concurrency to just return the results of tasks
    # But since we can't easily patch the imported function inside the module without knowing how it's imported,
    # we rely on mocking web3.eth.get_block returning a coroutine and the function running properly.
    # However, to avoid actually running concurrency logic, we can patch `gather_with_concurrency`.
    
    mock_block_data = {'number': 1}
    mock_web3.eth.get_block.return_value = mock_block_data

    with patch("ingestion.ethereumetl.jobs.export_blocks_job.gather_with_concurrency", new_callable=AsyncMock) as mock_gather:
        mock_gather.return_value = [mock_block_data, mock_block_data]
        
        batch = [1, 2]
        result = await job._fetch_batch(batch)
        
        assert result == [mock_block_data, mock_block_data]
        assert mock_web3.eth.get_block.call_count == 2

@pytest.mark.asyncio
async def test_process_batch(mock_web3, mock_item_exporter):
    job = ExportBlocksJob(
        start_block=0, end_block=10, batch_size=5, web3=mock_web3, max_workers=5, item_exporter=mock_item_exporter
    )
    
    # Mock block data
    mock_block_data = {
        'number': 1,
        'hash': b'0x123',
        'transactions': []
    }
    
    # Mock mapper
    with patch("ingestion.ethereumetl.jobs.export_blocks_job.EthBlockMapper") as MockMapper:
        mock_mapper_instance = MockMapper.return_value
        mock_block_model = EthBlock(number=1, type='block')
        mock_block_model.transactions = []
        mock_mapper_instance.web3_dict_to_block.return_value = mock_block_model
        
        # We need to set the instance on the job because it's initialized in __init__
        job.block_mapper = mock_mapper_instance
        
        await job._process_batch([mock_block_data])
        
        mock_mapper_instance.web3_dict_to_block.assert_called_with(mock_block_data)
        mock_item_exporter.export_item.assert_called_with(mock_block_model)

@pytest.mark.asyncio
async def test_process_batch_with_transactions(mock_web3, mock_item_exporter):
    job = ExportBlocksJob(
        start_block=0, end_block=10, batch_size=5, web3=mock_web3, max_workers=5, item_exporter=mock_item_exporter,
        export_transactions=True
    )
    
    mock_block_data = {'number': 1}
    
    with patch("ingestion.ethereumetl.jobs.export_blocks_job.EthBlockMapper") as MockMapper:
        mock_mapper = MockMapper.return_value
        
        # Block with transactions
        tx_mock = MagicMock()
        mock_block_model = MagicMock()
        mock_block_model.transactions = [tx_mock]
        mock_mapper.web3_dict_to_block.return_value = mock_block_model
        
        job.block_mapper = mock_mapper
        
        await job._process_batch([mock_block_data])
        
        # Should export block and transaction
        assert mock_item_exporter.export_item.call_count == 2
