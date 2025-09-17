import requests
from requests.exceptions import RequestException
from typing import Annotated, Any
from src.pipeline.config.config import config

class APIClient:
    def __init__(self):
        self.endpoint = config.api_config['base_url']

    def _get_data(self, startid: int) -> Any:
        try:
            base_url = self.endpoint  # Should be just: "https://export.arxiv.org/api/query"

            # Use a dict to pass query parameters
            params = {
                "search_query": "all:Physics",
                "start": startid,
                "max_results": 1
            }

            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.content
            return data.decode('utf-8')
        except RequestException as e:
            print(f"An error occurred while fetching data: {e}")
            return None
