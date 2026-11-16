from src.database.connection import conn


def get_all_rates():

    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            provider,
            base_currency,
            target_currency,
            exchange_rate,
            fetched_at
        FROM fx_rates
        ORDER BY id DESC
        LIMIT 100
    """)

    rows = cursor.fetchall()

    cursor.close()

    return rows