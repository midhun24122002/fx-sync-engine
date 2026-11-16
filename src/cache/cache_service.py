from src.cache.redis_client import redis_client


def cache_rate(
        base_currency,
        target_currency,
        exchange_rate
):

    key = f"{base_currency}_{target_currency}"

    redis_client.set(
        key,
        exchange_rate
    )