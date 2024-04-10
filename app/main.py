from fastapi import FastAPI, HTTPException,status
from datetime import datetime
from .database import connection
from .datasetmodel import Dataset,UpdateDataset
app = FastAPI()


@app.get("/v1/dataset/{dataset_id}")
def get_record(dataset_id):
    connection.cursor.execute("""SELECT * FROM datasets where dataset_id = %s """,(dataset_id,))
    dataset = connection.cursor.fetchone()
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dataset with dataset_id: {dataset_id} was not found")
    return {"dataset_details": dataset}

@app.get("/")
def get_all_records():
    connection.cursor.execute("""SELECT * FROM datasets""")
    dataset= connection.cursor.fetchall()
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="no records found")
    return {"records": dataset}

@app.post("/v1/dataset",status_code=status.HTTP_201_CREATED)
def create_records(dataset: Dataset):
    connection.cursor.execute("""insert into datasets (id,dataset_id,type,name,updated_date) values (%s,%s,%s,%s,%s) returning * """,(dataset.id,dataset.dataset_id,dataset.type,dataset.name,dataset.updated_date))
    new_dataset = connection.cursor.fetchone()
    connection.conn.commit()
    return {"dataset_inserted": new_dataset}

@app.patch("/v1/dataset/{dataset_id}")
def update_record(dataset_id,dataset: UpdateDataset):
    update_fields = ", ".join(f"{field}=%s" for field in dataset.model_dump(exclude_unset=True))
    update_values = [getattr(dataset, field) for field in dataset.model_dump(exclude_unset=True)]
    update_values.append(dataset_id)
    connection.cursor.execute(f"""UPDATE datasets SET {update_fields} WHERE dataset_id = %s RETURNING *""", update_values)
    updated_dataset = connection.cursor.fetchone()
    connection.conn.commit()
    #cursor.execute("""UPDATE datasets set name=%s WHERE dataset_id=%s returning *""",(record.name,dataset_id))
    #updated_record = cursor.fetchone()
    #conn.commit()
    if updated_dataset == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dataset with dataset_id: {dataset_id} does not exist")
    return {"updated_dataset_id": dataset_id}

@app.delete("/v1/dataset/{dataset_id}")
def delete_record(dataset_id):
    connection.cursor.execute("""DELETE FROM datasets where id = %s returning *""",(dataset_id,))
    deleted_record=connection.cursor.fetchone()
    connection.conn.commit()
    if deleted_record ==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dataset with dataset_id: {dataset_id} does not exist")
    return {"deleted_dataset_id": dataset_id}