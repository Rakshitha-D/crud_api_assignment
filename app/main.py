from fastapi import FastAPI, HTTPException,status
from datetime import datetime
from .database import connection
from .datasetmodel import Dataset,UpdateDataset
import json
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
    updated_date = datetime.now()
    insert_fields=", ".join(f"{field}" for field in dataset.model_dump())
    print(insert_fields)
    print("length:",len(insert_fields))
    #insert_values = [json.dumps(getattr(dataset, field)) for field in dataset.model_dump() if isinstance(getattr(dataset, field), dict)]
    insert_values = []
    for field in dataset.model_dump():
        attribute = getattr(dataset, field)
        if isinstance(attribute, dict):
            insert_values.append(json.dumps(attribute))
        else:
            insert_values.append(attribute)

    insert_values.append(updated_date)
    #connection.cursor.execute("""insert into datasets (id,dataset_id,type,name,validation_config,extraction_config,dedup_config,data_schema,denorm_config,router_config,dataset_config,status,tags,data_version,created_by,updated_by,updated_date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) returning * """,insert_values)
    connection.cursor.execute(f"""insert into datasets ({insert_fields},updated_date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) returning * """,insert_values)
    new_dataset = connection.cursor.fetchone()
    connection.conn.commit()
    return {"dataset_id": dataset.dataset_id}

@app.patch("/v1/dataset/{dataset_id}")
def update_record(dataset_id,dataset: UpdateDataset):
    update_fields = ", ".join(f"{field}=%s" for field in dataset.model_dump(exclude_unset=True))
    #update_values = [getattr(dataset, field) for field in dataset.model_dump(exclude_unset=True)]
    update_values = []
    for field in dataset.model_dump(exclude_unset=True):
        attribute = getattr(dataset, field)
        if isinstance(attribute, dict):
            update_values.append(json.dumps(attribute))
        else:
            update_values.append(attribute)

    updated_date= datetime.now()
    update_values.append(updated_date)
    update_values.append(dataset_id)
    connection.cursor.execute(f"""UPDATE datasets SET {update_fields},updated_date = %s WHERE dataset_id = %s RETURNING *""", update_values)
    updated_dataset = connection.cursor.fetchone()
    connection.conn.commit()
    #cursor.execute("""UPDATE datasets set name=%s WHERE dataset_id=%s returning *""",(record.name,dataset_id))
    #updated_record = cursor.fetchone()
    #conn.commit()
    if updated_dataset == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dataset with dataset_id: {dataset_id} does not exist")
    return {"dataset_id": dataset_id}

@app.delete("/v1/dataset/{dataset_id}")
def delete_record(dataset_id):
    connection.cursor.execute("""DELETE FROM datasets where id = %s returning *""",(dataset_id,))
    deleted_record=connection.cursor.fetchone()
    connection.conn.commit()
    if deleted_record ==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"dataset with dataset_id: {dataset_id} does not exist")
    return {"dataset_id": dataset_id}