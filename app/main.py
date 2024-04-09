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
def update_record(dataset_id,record: Record):
    cursor.execute("""UPDATE datasets set name=%s WHERE dataset_id=%s """,(record.name,dataset_id))
    updated_record = cursor.fetchone()
    conn.commit()
    if updated_record == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id: {id} does not exist")
    return {"record": updated_record}

@app.delete("/v1/dataset/{id}")
def delete_record(id):
    cursor.execute("""DELETE FROM datasets where id = %s returning *""",(id,))
    deleted_post=cursor.fetchone()
    conn.commit()
    return {"deleted_post": deleted_post}