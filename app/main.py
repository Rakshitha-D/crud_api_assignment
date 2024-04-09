import datetime
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Record(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str
    updated_date: datetime
    

@app.get("/v1/dataset/{dataset_id}")
def get_record():
    return {"Hello": "World"}