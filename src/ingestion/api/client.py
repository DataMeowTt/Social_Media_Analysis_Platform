import requests
import os
from dotenv import load_dotenv

load_dotenv()

class TwitterAPIClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url

    def get(self, endpoint: str, params:dict = None):
        url = f"{self.base_url}{endpoint}"
        headers = {
            "X-API-Key": self.api_key
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None