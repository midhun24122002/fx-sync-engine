import requests


class FrankfurterClient:

    URL = "https://api.frankfurter.app/latest?from=USD"

    def get_latest_rates(self):

        response = requests.get(
            self.URL,
            timeout=30
        )

        response.raise_for_status()

        return response.json()