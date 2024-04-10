from fastapi import FastAPI, HTTPException,status
from datetime import datetime
from .database import connection
from .datasetmodel import Record,UpdateRecord
app = FastAPI()


@app.get("/v1/dataset/{dataset_id}")
def get_record(dataset_id):
    connection.cursor.execute("""SELECT * FROM datasets where dataset_id = %s """,(dataset_id,))
    record = connection.cursor.fetchone()
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id: {dataset_id} was not found")
    return {"record_details": record}

@app.get("/")
def get_all_records():
    connection.cursor.execute("""SELECT * FROM datasets""")
    records= connection.cursor.fetchall()
    if not records:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="no records found")
    return {"records": records}

@app.post("/v1/dataset",status_code=status.HTTP_201_CREATED)
def create_records(record: Record):
    connection.cursor.execute("""insert into datasets (id,dataset_id,type,name,updated_date) values (%s,%s,%s,%s,%s) returning * """,(record.id,record.dataset_id,record.type,record.name,record.updated_date))
    new_record = connection.cursor.fetchone()
    connection.conn.commit()
    return {"record_inserted": new_record}

@app.patch("/v1/dataset/{dataset_id}")
def update_record(dataset_id,record: UpdateRecord):
    update_fields = ", ".join(f"{field}=%s" for field in record.model_dump(exclude_unset=True))
    update_values = [getattr(record, field) for field in record.model_dump(exclude_unset=True)]
    update_values.append(dataset_id)
    connection.cursor.execute(f"""UPDATE datasets SET {update_fields} WHERE dataset_id = %s RETURNING *""", update_values)
    updated_record = connection.cursor.fetchone()
    connection.conn.commit()
    #cursor.execute("""UPDATE datasets set name=%s WHERE dataset_id=%s returning *""",(record.name,dataset_id))
    #updated_record = cursor.fetchone()
    #conn.commit()
    if updated_record == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with id: {id} does not exist")
    return {"record": updated_record}

@app.delete("/v1/dataset/{dataset_id}")
def delete_record(dataset_id):
    connection.cursor.execute("""DELETE FROM datasets where id = %s returning *""",(dataset_id,))
    deleted_record=connection.cursor.fetchone()
    connection.conn.commit()
    if deleted_record ==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"record with dataset_id: {dataset_id} does not exist")
    return {"deleted_record": deleted_record}