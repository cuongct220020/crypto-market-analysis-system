import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from ingestion.ethereumetl.jobs.export_traces_job import ExportTracesJob
from constants.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER

@pytest.fixture
def mock_web3():
    web3 = MagicMock()
    web3.provider = MagicMock()
    web3.provider.make_request = AsyncMock()
    return web3

@pytest.fixture
def mock_item_exporter():
    return MagicMock()

@pytest.fixture
def mock_job(mock_web3, mock_item_exporter):
    return ExportTracesJob(
        start_block=0,
        end_block=10,
        batch_size=1,
        web3=mock_web3,
        item_exporter=mock_item_exporter,
        max_workers=1
    )

def test_initialization_validation(mock_web3, mock_item_exporter):
    with pytest.raises(ValueError):
        ExportTracesJob(start_block=-1, end_block=0, batch_size=1, web3=mock_web3, item_exporter=mock_item_exporter, max_workers=1)

@pytest.mark.asyncio
async def test_fetch_and_export_traces_success(mock_job, mock_web3, mock_item_exporter):
    mock_response = {'result': [{'trace': 'data'}]}
    mock_web3.provider.make_request.return_value = mock_response
    
    with patch("ingestion.ethereumetl.jobs.export_traces_job.EthTraceMapper") as MockMapper:
        mock_mapper = MockMapper.return_value
        mock_trace_model = MagicMock()
        mock_mapper.json_dict_to_trace.return_value = mock_trace_model
        
        mock_job.trace_mapper = mock_mapper
        
        await mock_job._fetch_and_export_traces(100)
        
        mock_web3.provider.make_request.assert_called_with("trace_block", [hex(100)])
        mock_item_exporter.export_item.assert_called_with(mock_trace_model)

@pytest.mark.asyncio
async def test_fetch_and_export_traces_rpc_error(mock_job, mock_web3):
    mock_response = {'error': 'Some RPC error'}
    mock_web3.provider.make_request.return_value = mock_response
    
    with pytest.raises(ValueError, match="RPC Error in trace_block"):
        await mock_job._fetch_and_export_traces(100)

@pytest.mark.asyncio
async def test_fetch_and_export_traces_none_result(mock_job, mock_web3):
    mock_response = {'result': None}
    mock_web3.provider.make_request.return_value = mock_response
    
    with pytest.raises(ValueError, match="Response from the node is None"):
        await mock_job._fetch_and_export_traces(100)

@pytest.mark.asyncio
async def test_special_traces_genesis(mock_job):
    mock_job.include_genesis_traces = True
    
    # Mock make_request to return empty list to focus on special traces
    mock_job.web3.provider.make_request.return_value = {'result': []}
    
    with patch("ingestion.ethereumetl.jobs.export_traces_job.EthSpecialTraceService") as MockService:
        mock_service = MockService.return_value
        mock_trace = MagicMock()
        mock_service.get_genesis_traces.return_value = [mock_trace]
        
        mock_job.special_trace_service = mock_service
        
        await mock_job._fetch_and_export_traces(0)
        
        mock_service.get_genesis_traces.assert_called_once()
        mock_job.item_exporter.export_item.assert_called_with(mock_trace)

@pytest.mark.asyncio
async def test_special_traces_daofork(mock_job):
    mock_job.include_daofork_traces = True
    
    mock_job.web3.provider.make_request.return_value = {'result': []}
    
    with patch("ingestion.ethereumetl.jobs.export_traces_job.EthSpecialTraceService") as MockService:
        mock_service = MockService.return_value
        mock_trace = MagicMock()
        mock_service.get_daofork_traces.return_value = [mock_trace]
        
        mock_job.special_trace_service = mock_service
        
        await mock_job._fetch_and_export_traces(DAOFORK_BLOCK_NUMBER)
        
        mock_service.get_daofork_traces.assert_called_once()
        mock_job.item_exporter.export_item.assert_called_with(mock_trace)
