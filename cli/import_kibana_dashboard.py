import click
import requests
import os
from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Import Kibana Dashboard")

@click.command()
@click.option(
    "-f",
    "--file-path",
    default="infrastructure/elastic-cluster/dashboards/crypto_dashboard_skeleton.ndjson",
    show_default=True,
    type=str,
    help="Path to the .ndjson file containing Kibana saved objects."
)
@click.option(
    "--host",
    default=f"http://{configs.elasticsearch.host.replace('elasticsearch', 'localhost')}:5601", # Hack for running outside docker vs inside
    show_default=True,
    type=str,
    help="Kibana base URL (e.g. http://localhost:5601)"
)
def import_kibana_dashboard(file_path: str, host: str):
    """
    Imports Saved Objects (Dashboards, Index Patterns) into Kibana from an .ndjson file.
    """
    logger.info(f"Starting Kibana dashboard import from {file_path} to {host}...")

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    # Kibana Import API
    url = f"{host}/api/saved_objects/_import?overwrite=true"
    
    # Headers required by Kibana
    headers = {
        "kbn-xsrf": "true",
    }

    try:
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f, 'application/x-ndjson')}
            
            response = requests.post(url, headers=headers, files=files)
            
            if response.status_code == 200:
                res_json = response.json()
                if res_json.get("success"):
                    logger.info(f"Successfully imported {res_json.get('successCount')} objects.")
                else:
                    logger.warning(f"Import completed with warnings: {res_json}")
            else:
                logger.error(f"Failed to import. Status: {response.status_code}, Body: {response.text}")

    except Exception as e:
        logger.exception(f"An error occurred during import: {e}")
