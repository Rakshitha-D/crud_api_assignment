from pydantic import BaseModel
from typing import Optional

class Dataset(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str
    updated_date: str

class UpdateDataset(BaseModel):
    id: Optional[str]= None
    dataset_id: str | None = None
    type: str | None = None
    name: str | None = None
    updated_date: str 