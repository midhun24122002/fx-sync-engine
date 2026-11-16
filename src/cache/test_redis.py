from src.cache.redis_client import redis_client

redis_client.set("USD_INR", 87.25)

value = redis_client.get("USD_INR")

print(value)