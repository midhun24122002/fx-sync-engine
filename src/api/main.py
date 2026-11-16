from fastapi import FastAPI

from src.database.query_repository import get_all_rates

app = FastAPI()


@app.get("/")
def home():

    return {
        "project": "FX Sync Engine",
        "status": "running"
    }


@app.get("/health")
def health():

    return {
        "status": "healthy"
    }


@app.get("/rates")
def rates():

    rows = get_all_rates()

    results = []

    for row in rows:

        results.append(
            {
                "provider": row[0],
                "base_currency": row[1],
                "target_currency": row[2],
                "exchange_rate": float(row[3]),
                "fetched_at": str(row[4])
            }
        )

    return results