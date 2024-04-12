import json
from pydantic import BaseModel,Field
from typing_extensions import Any, Dict, List, Optional, TypedDict, Union
from datetime import datetime


class ValidationConfig(BaseModel):
    validate: bool
    mode: str
    validation_mode: str

class DedupConfig(BaseModel):
    drop_duplicates: bool
    dedup_key: str
    dedup_period: int

class ExtractionConfig(BaseModel):
    is_batch_event: bool
    extraction_key: str
    dedup_config: DedupConfig
    batch_id: str

class Dataset(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str
    validation_config: object
    extraction_config: object
    dedup_config : object
    data_schema: object
    denorm_config: object
    router_config: object
    dataset_config: object
    status: str
    tags: List[str]
    data_version: int
    created_by: str
    updated_by: str

class UpdateDataset(BaseModel):
    #id: Optional[str]= None
    dataset_id: str | None = None
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

class Response(BaseModel):
    id: str
    ver: str
    ts: datetime
    params: object
    responseCode: str
    result: object