import json

from kafka import KafkaConsumer

from src.cache.cache_service import cache_rate
from src.database.fx_repository import save_rate
from src.validators.fx_validator import validate_rate


consumer = KafkaConsumer(
    "fx-rates",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started...")

for message in consumer:

    data = message.value

    print(f"Received: {data}")

    try:

        provider = data["provider"]

        base_currency = data["base_currency"]

        target_currency = data["target_currency"]

        exchange_rate = data["exchange_rate"]

        validate_rate(data)

        cache_rate(
            base_currency,
            target_currency,
            exchange_rate
        )

        save_rate(
            provider,
            base_currency,
            target_currency,
            exchange_rate
        )

        print(
            f"Saved {base_currency}/{target_currency}"
        )

    except Exception as e:

        print(
            f"Validation Failed: {e}"
        )