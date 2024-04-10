from pydantic import BaseModel,Field
from typing import Any, Dict, List, Optional
from datetime import datetime

class Dataset(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str
    validation_config: dict
    extraction_config: dict
    dedup_config: dict
    data_schema: dict
    denorm_config: dict
    router_config: dict
    dataset_config: dict
    status: str
    tags: List[str]
    data_version: int
    created_by: str
    updated_by: str

class UpdateDataset(BaseModel):
    #id: Optional[str]= None
    #dataset_id: str | None = None
    type: str | None = None
    name: str | None = None
    validation_config: dict | None = None
    extraction_config: dict | None = None
    dedup_config: dict | None = None
    data_schema: dict | None = None
    denorm_config: dict | None = None
    router_config: dict | None = None
    dataset_config: dict | None = None
    status: str | None = None
    tags: List[str] | None = None
    data_version: int | None = None
    created_by: str | None = None
    updated_by: str | None = None