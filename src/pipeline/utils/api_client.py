import requests
from requests.exceptions import RequestException
from typing import Dict, Any
from src.pipeline.config.config import config

class APIClient:
    def __init__(self, endpoint:str):
        self.endpoint = config.api_config['base_url']

    def _get_data(self) -> Any :
        try:
            response = requests.get(self.endpoint)
            response.raise_for_status()  # Raise an error for bad responses
            return response.json()
        except RequestException as e:
            print(f"An error occurred while fetching data: {e}")
            return None
