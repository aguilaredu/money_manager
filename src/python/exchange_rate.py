import os 
from utils import load_config
import requests
import json 
import pandas as pd

class ExchangeRates():
    def __init__(self, base_path: str, output_size: str='full', symbol: str='USDHNL',) -> None:
        self.output_size = output_size
        self.symbol = symbol
        self.base_path = base_path

    def get_json_data(self):
        """Get json data 
        """
        secrets_dir = os.path.join(self.base_path, 'secrets/exchange_rate_api.json')
        api_secrets = load_config(secrets_dir)
        endpoint = api_secrets['endpoint']
        api_key = api_secrets['api_key']
        payload = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': self.symbol,
            'apikey': api_key,
            'outputsize': self.output_size
        }
        try:
            r = requests.get(endpoint, params=payload)
            response = json.loads(r.text)['Time Series (Daily)']
        except Exception as e:
            print(f"Error fetching exchange rates. Either there is no internet connection or the API limit was reached.  Error: {e}.")
            return None
        
        exchange_rates = pd.DataFrame(response).T

        exchange_rates.index = pd.to_datetime(exchange_rates.index)
        exchange_rates.rename(columns={'4. close': 'exchange_rate'}, inplace=True)
        exchange_rates = exchange_rates['exchange_rate']

        return exchange_rates
    
    def get_exchange_rate_dataframe(self):
        return self.get_json_data()
