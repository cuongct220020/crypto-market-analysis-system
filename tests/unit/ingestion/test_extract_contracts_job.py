import pytest
from unittest.mock import MagicMock, patch
from ingestion.ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ingestion.ethereumetl.models.contract import EthContract

@pytest.fixture
def mock_item_exporter():
    return MagicMock()

def test_extract_contracts_logic(mock_item_exporter):
    # Setup Traces
    trace_create = MagicMock()
    trace_create.trace_type = "create"
    trace_create.to_address = "0xContract"
    trace_create.status = 1
    trace_create.output = "0xbytecode"
    trace_create.block_number = 100
    
    trace_failed = MagicMock()
    trace_failed.trace_type = "create"
    trace_failed.status = 0 # Failed
    
    trace_call = MagicMock()
    trace_call.trace_type = "call" # Not create
    
    traces = [trace_create, trace_failed, trace_call]
    
    job = ExtractContractsJob(traces_iterable=traces, item_exporter=mock_item_exporter)
    
    with patch("ingestion.ethereumetl.jobs.extract_contracts_job.EthContractService") as MockService:
        mock_service = MockService.return_value
        mock_service.get_function_sighashes.return_value = ["0xabc"]
        mock_service.is_erc20_contract.return_value = True
        mock_service.is_erc721_contract.return_value = False
        
        job.contract_service = mock_service
        
        job._extract_contracts(traces)
        
        # Only 1 contract should be extracted
        assert mock_item_exporter.export_item.call_count == 1
        
        exported_contract = mock_item_exporter.export_item.call_args[0][0]
        assert isinstance(exported_contract, EthContract)
        assert exported_contract.address == "0xContract"
        assert exported_contract.is_erc20 is True
        assert exported_contract.is_erc721 is False
