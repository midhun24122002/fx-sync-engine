from clients.frankfurter_client import FrankfurterClient

client = FrankfurterClient()

data = client.get_rates()

print(data)