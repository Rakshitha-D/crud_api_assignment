from fastapi import FastAPI, HTTPException,status
from datetime import datetime
from jsonschema import ValidationError
from .database import connection
from .datasetmodel import UpdateDataset,Dataset
import json
app = FastAPI()

def get_dataset_id(dataset_id)->bool:
    connection.cursor.execute("""SELECT * from datasets where dataset_id = %s """,(dataset_id,))
    if connection.cursor.fetchone() is not None:
        return True
    return False

@app.get("/v1/dataset/{dataset_id}")
def get_dataset(dataset_id):
    connection.cursor.execute("""SELECT * FROM datasets where dataset_id = %s """,(dataset_id,))
    dataset = connection.cursor.fetchone()
    if not dataset:
        response = {
            "id": "api.dataset.read",
            "ver": "1.0",
            "ts": datetime.now().isoformat() + "Z",
            "params": {
            "err": "DATASET_NOT_FOUND",
            "status": "Failed",
            "errmsg": "No dataset found with id: "+ dataset_id
            },
            "responseCode": "NOT_FOUND",
            "result": {}
        }
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=response)
    return { "id": "api.dataset.read",
            "ver": "1.0", 
            "ts": datetime.now().isoformat() + "Z",
            "params": {
                "err": "null",
                "status": "successful",
                "errmsg": "null"
                },
                "responseCode": "OK",
                "result": dataset
            }

@app.get("/")
def get_all_datasets():
    connection.cursor.execute("""SELECT * FROM datasets""")
    dataset= connection.cursor.fetchall()
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="no records found")
    return {"records": dataset}

@app.post("/v1/dataset")
def create_dataset(dataset: Dataset):
    try:
        if not get_dataset_id(dataset.dataset_id):
            updated_date = datetime.now()
            insert_fields=", ".join(f"{field}" for field in dataset.model_dump())
            insert_values = []
            for field in dataset.model_dump():
                attribute = getattr(dataset, field)
                if isinstance(attribute, dict):
                    insert_values.append(json.dumps(attribute))
                else:
                    insert_values.append(attribute)

            insert_values.append(updated_date)
            connection.cursor.execute(f"""insert into datasets ({insert_fields},updated_date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) returning * """,insert_values)
            new_dataset = connection.cursor.fetchone()
            connection.conn.commit()
            return {"id": "api.dataset.create",
            "ver": "1.0",
            "ts": datetime.now().isoformat() + "Z",
            "params": {
                "err": "null",
                "status": "successful",
                "errmsg": "null"
            },
            "responseCode": "OK",
            "result": {
                "id": dataset.dataset_id
            }
            }
        elif get_dataset_id(dataset.dataset_id):
            response = {
                "id": "api.dataset.create",
                "ver": "1.0",
                "ts": datetime.now().isoformat() + "Z",
                "params": {
                "err": "DATASET_NOT_CREATED",
                "status": "Failed",
                "errmsg": "Dataset already exists"
                },
                "responseCode": "CONFLICT",
                "result": {}
            }
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail=response)
    except HTTPException:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,detail=response)
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        

@app.patch("/v1/dataset/{dataset_id}")
def update_dataset(dataset_id,dataset: UpdateDataset):
    try:
        if get_dataset_id(dataset_id):
            update_fields = ", ".join(f"{field}=%s" for field in dataset.model_dump(exclude_unset=True))
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
            connection.cursor.fetchone()
            connection.conn.commit()
            return {"id": "api.dataset.update",
            "ver": "1.0",
            "ts": datetime.now().isoformat() + "Z",
            "params": {
                "err": "null",
                "status": "successful",
                "errmsg": "null"
            },
            "responseCode": "OK",
            "result": {
                "id": dataset_id
            }
            }
        else:
            response = {
            "id": "api.dataset.update",
            "ver": "1.0",
            "ts": datetime.now().isoformat() + "Z",
            "params": {
            "err": "DATASET_NOT_FOUND",
            "status": "Failed",
            "errmsg": "No records found"
            },
            "responseCode": "NOT_FOUND",
                "result": {}
            }
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=response)
    except HTTPException:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=response)
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            

@app.delete("/v1/dataset/{dataset_id}")
def delete_dataset(dataset_id):
    try:
        if not get_dataset_id(dataset_id):
            response = {
            "id": "api.dataset.delete",
            "ver": "1.0",
            "ts": datetime.now().isoformat() + "Z",
            "params": {
            "err": "DATASET_NOT_FOUND",
            "status": "Failed",
            "errmsg": "No records found"
            },
            "responseCode": "NOT_FOUND",
            "result": {}
            }
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=response)
        else:
            connection.cursor.execute("""DELETE FROM datasets where id = %s returning *""",(dataset_id,))
            deleted_record=connection.cursor.fetchone()
            connection.conn.commit()
            return {"id": "api.dataset.delete",
            "ver": "1.0",
            "ts": datetime.now().isoformat() + "Z",
            "params": {
                "err": "null",
                "status": "successful",
                "errmsg": "null"
                },
            "responseCode": "OK",
            "result": {
                "id": dataset_id
                }
            }
    except HTTPException:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=response)
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    