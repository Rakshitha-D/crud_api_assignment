from typing import Optional
from fastapi import FastAPI, HTTPException,status
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

app = FastAPI()

class Record(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str
    updated_date: str

class UpdateRecord(BaseModel):
    id: str | None = None
    dataset_id: str | None = None
    type: str | None = None
    name: str | None = None
    updated_date: str 

conn = psycopg2.connect(host='localhost',database='obsrv',user='postgres',password='drakshitha',cursor_factory=RealDictCursor)
cursor = conn.cursor()
print("Database connection was succesfull")

@app.get("/v1/dataset/{dataset_id}")
def get_record(dataset_id):
    cursor.execute("""SELECT * FROM datasets where dataset_id = %s """,(dataset_id,))
    record = cursor.fetchone()
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id: {dataset_id} was not found")
    return {"record_details": record}

@app.get("/")
def get_all_records():
    cursor.execute("""SELECT * FROM datasets""")
    records= cursor.fetchall()
    if not records:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="no records found")
    return {"records": records}

@app.post("/v1/dataset")
def create_records(record: Record):
    cursor.execute("""insert into datasets (id,dataset_id,type,name,updated_date) values (%s,%s,%s,%s,%s) returning * """,(record.id,record.dataset_id,record.type,record.name,record.updated_date))
    new_record = cursor.fetchone()
    conn.commit()
    return {"record_inserted": new_record}

@app.patch("/v1/dataset/{dataset_id}")
def update_record(dataset_id,record: UpdateRecord):
    update_fields = ", ".join(f"{field}=%s" for field in record.model_dump(exclude_unset=True))
    update_values = [getattr(record, field) for field in record.model_dump(exclude_unset=True)]
    update_values.append(dataset_id)
    print(update_fields)
    print(update_values)
    cursor.execute(f"""UPDATE datasets SET {update_fields} WHERE dataset_id = %s RETURNING *""", update_values)
    updated_record = cursor.fetchone()
    conn.commit()
    #cursor.execute("""UPDATE datasets set name=%s WHERE dataset_id=%s returning *""",(record.name,dataset_id))
    #updated_record = cursor.fetchone()
    #conn.commit()
    if updated_record == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id: {id} does not exist")
    return {"record": updated_record}

@app.delete("/v1/dataset/{dataset_id}")
def delete_record(dataset_id):
    cursor.execute("""DELETE FROM datasets where id = %s returning *""",(dataset_id,))
    deleted_post=cursor.fetchone()
    conn.commit()
    return {"deleted_post": deleted_post}