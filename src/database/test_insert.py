# src/database/test_insert.py

from src.database.fx_repository import save_rate

save_rate(
    provider="TEST",
    base_currency="USD",
    target_currency="INR",
    exchange_rate=87.25
)

print("Inserted")