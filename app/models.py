from .database import Base
from sqlalchemy import ARRAY, TIMESTAMP, Column,Integer,String,Boolean,JSON

class Datasets(Base):
    __tablename__ = "datasets"
    
    id = Column(String,primary_key=True)
    dataset_id = Column(String)
    type = Column(String,nullable=False)
    name = Column(String)
    validation_config = Column(JSON)
    extraction_config = Column(JSON)
    dedup_config = Column(JSON)
    data_schema = Column(JSON)
    denorm_config = Column(JSON)
    router_config = Column(JSON)
    dataset_config = Column(JSON)
    status = Column(String)
    tags = Column(ARRAY(String))
    data_version = Column(Integer)
    created_by = Column(String)
    updated_by = Column(String)
    created_date = Column(TIMESTAMP(timezone=True),nullable=False,server_default=text('now()'))
    updated_date = Column(TIMESTAMP(timezone=True),nullable=False,server_default=text('now()'))
    published_date = Column(TIMESTAMP(timezone=True),nullable=False,server_default=text('now()'))
