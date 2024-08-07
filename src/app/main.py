from fastapi import FastAPI, HTTPException
from models import Transaction, TransactionPrediction, PredictionResponse
from typing import List

from config import Config
from inference import InferenceModel

import uvicorn

import traceback


app = FastAPI()
predictor = InferenceModel()

@app.post("/api/predict")
async def predict(transactions: List[Transaction]):
    try:
        prediction = predictor.predict_list(transactions)
        response = [
            TransactionPrediction(transaction_id=transaction.transaction_id, prediction=pred)
            for transaction, pred in zip(transactions, prediction)
        ]
        return PredictionResponse(data=response)
    except Exception as e:
        _traceback = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}, {_traceback}")

@app.get("/api/health")
async def health():
    return {"status": "healthy"}

@app.get("/api/ready")
async def ready():
    # Add any readiness check logic here
    return {"status": "ready"}

if __name__ == "__main__":
    config = Config()
    uvicorn.run(
        app,
        host=config.backend_host,
        port=config.backend_port,
    )