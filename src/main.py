from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {
        "project": "FX Sync Engine",
        "status": "running"
    }