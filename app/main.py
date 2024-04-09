import datetime
from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

class Record(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str


@app.get("/v1/dataset/{dataset_id}")
def get_record():
    return {"Hello": "World"}