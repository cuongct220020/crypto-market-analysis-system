import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from ingestion.ethereumetl.jobs.export_tokens_job import ExportTokensJob

@pytest.fixture
def mock_web3():
    return MagicMock()

@pytest.fixture
def mock_item_exporter():
    return MagicMock()

@pytest.fixture
def mock_batch_work_executor():
    with patch("ingestion.ethereumetl.jobs.export_tokens_job.AsyncBatchWorkExecutor") as MockExecutor:
        mock_instance = MockExecutor.return_value
        mock_instance.execute = AsyncMock()
        mock_instance.shutdown = MagicMock()
        yield mock_instance

@pytest.mark.asyncio
async def test_export_tokens_parsing_str(mock_web3, mock_item_exporter):
    job = ExportTokensJob(
        web3=mock_web3,
        item_exporter=mock_item_exporter,
        token_addresses_iterable=["0x123"],
        max_workers=1
    )
    
    with patch("ingestion.ethereumetl.jobs.export_tokens_job.EthTokenMetadataService") as MockService:
        mock_service = MockService.return_value
        mock_service.get_token = AsyncMock()
        mock_token_obj = MagicMock()
        mock_service.get_token.return_value = mock_token_obj
        
        job.token_service = mock_service
        
        with patch("ingestion.ethereumetl.jobs.export_tokens_job.gather_with_concurrency", new_callable=AsyncMock) as mock_gather:
            await job._export_tokens(["0x123"])
            
            # Verify gather was called
            mock_gather.assert_called_once()
            
            # Since we can't inspect the coroutines passed to gather easily in this mock structure without running them,
            # we rely on testing _export_token separately or mocking the call flow.
            # But here we mocked gather, so the internal tasks aren't awaited by gather.
            # We should probably invoke _export_token directly to test logic.

@pytest.mark.asyncio
async def test_export_token_logic(mock_web3, mock_item_exporter):
    job = ExportTokensJob(
        web3=mock_web3,
        item_exporter=mock_item_exporter,
        token_addresses_iterable=[],
        max_workers=1
    )
    
    mock_token = MagicMock()
    mock_token_dict = {"address": "0x123"}
    
    with patch("ingestion.ethereumetl.jobs.export_tokens_job.EthTokenMetadataService") as MockService:
        mock_service = MockService.return_value
        mock_service.get_token = AsyncMock(return_value=mock_token)
        job.token_service = mock_service
        
        job.token_mapper.token_to_dict = MagicMock(return_value=mock_token_dict)
        
        await job._export_token("0x123", block_number=100)
        
        mock_service.get_token.assert_called_with("0x123")
        assert mock_token.block_number == 100
        job.token_mapper.token_to_dict.assert_called_with(mock_token)
        mock_item_exporter.export_item.assert_called_with(mock_token_dict)

@pytest.mark.asyncio
async def test_export_tokens_varied_input(mock_web3, mock_item_exporter):
    job = ExportTokensJob(
        web3=mock_web3,
        item_exporter=mock_item_exporter,
        token_addresses_iterable=[],
        max_workers=1
    )
    
    inputs = [
        "0x1", 
        {"address": "0x2", "block_number": 2}, 
        ("0x3", 3)
    ]
    
    with patch.object(job, '_export_token', new_callable=AsyncMock) as mock_export_method:
        with patch("ingestion.ethereumetl.jobs.export_tokens_job.gather_with_concurrency", new_callable=AsyncMock) as mock_gather:
            await job._export_tokens(inputs)
            
            # We expect gather to be called with 3 tasks
            args, _ = mock_gather.call_args
            assert args[0] == 5 # default max_concurrent_requests
            assert len(args) == 4 # 1 arg for concurrency + 3 tasks
