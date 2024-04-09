import datetime
from fastapi import FastAPI, HTTPException,status
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

class Record(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str

conn = psycopg2.connect(host='localhost',database='obsrv',user='postgres',password='drakshitha',cursor_factory=RealDictCursor)
cursor = conn.cursor()
print("Database connection was succesfull")

@app.get("/v1/dataset/{dataset_id}")
def get_record(dataset_id):
    print(type(dataset_id))
    cursor.execute(f"""SELECT * FROM datasets where dataset_id = '{dataset_id}' """)
    record = cursor.fetchone()
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id: {dataset_id} was not found")
    return {"record_details": record}