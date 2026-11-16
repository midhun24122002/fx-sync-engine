import json

from kafka import KafkaProducer

from src.clients.frankfurter_client import FrankfurterClient


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def publish_fx_rates():

    client = FrankfurterClient()

    data = client.get_latest_rates()

    base_currency = data["base"]

    for target_currency, rate in data["rates"].items():

        message = {
            "provider": "Frankfurter",
            "base_currency": base_currency,
            "target_currency": target_currency,
            "exchange_rate": rate
        }

        producer.send(
            "fx-rates",
            message
        )

    producer.flush()

    print("FX Rates Published")


if __name__ == "__main__":
    publish_fx_rates()