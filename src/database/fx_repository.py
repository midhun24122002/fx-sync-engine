from src.database.connection import conn


def save_rate(
    provider,
    base_currency,
    target_currency,
    exchange_rate
):

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO fx_rates
        (
            provider,
            base_currency,
            target_currency,
            exchange_rate
        )
        VALUES
        (%s, %s, %s, %s)
        """,
        (
            provider,
            base_currency,
            target_currency,
            exchange_rate
        )
    )

    conn.commit()

    cursor.close()