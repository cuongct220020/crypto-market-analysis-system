import pytest
from unittest.mock import MagicMock, patch
from ingestion.ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob

@pytest.fixture
def mock_item_exporter():
    return MagicMock()

def test_extract_transfers_logic(mock_item_exporter):
    logs = ["log1", "log2"]
    
    job = ExtractTokenTransfersJob(logs_iterable=logs, item_exporter=mock_item_exporter)
    
    with patch("ingestion.ethereumetl.jobs.extract_token_transfers_job.EthTokenTransferService") as MockService:
        mock_service = MockService.return_value
        
        # Mock extracted transfer
        mock_transfer = MagicMock()
        
        # First log returns a transfer, second returns None
        mock_service.extract_transfer_from_log.side_effect = [mock_transfer, None]
        
        job.token_transfer_service = mock_service
        
        job._extract_transfers(logs)
        
        # Should have called extract twice
        assert mock_service.extract_transfer_from_log.call_count == 2
        
        # Should have exported once (only the valid one)
        mock_item_exporter.export_item.assert_called_once_with(mock_transfer)
