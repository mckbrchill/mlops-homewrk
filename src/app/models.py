from pydantic import BaseModel, Field
from typing import Optional, List

class Transaction(BaseModel):
    transaction_id: int
    tx_datetime: str
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int

class TransactionPrediction(BaseModel):
    transaction_id: int
    prediction: float

class PredictionResponse(BaseModel):
    data: Optional[List[TransactionPrediction]] = None
    error: Optional[str] = None