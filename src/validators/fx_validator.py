def validate_rate(message):

    required_fields = [
        "provider",
        "base_currency",
        "target_currency",
        "exchange_rate"
    ]

    for field in required_fields:

        if field not in message:
            raise ValueError(
                f"Missing field: {field}"
            )

    exchange_rate = message["exchange_rate"]

    if exchange_rate is None:
        raise ValueError(
            "Exchange rate cannot be null"
        )

    if exchange_rate <= 0:
        raise ValueError(
            "Exchange rate must be positive"
        )

    return True