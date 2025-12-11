from datetime import datetime

from pydantic import BaseModel


class RequestBase(BaseModel):
    pass


class RequestCreateDB(RequestBase):
    pubg_id: str


class RequestUpdateDB(BaseModel):
    is_active: bool | None = None
    statistics: list[dict] | None = None


class RequestCreate(BaseModel):
    pubg_id: str


class RequestResponse(BaseModel):
    id: int
    pubg_id: str
    is_active: bool
    statistics: list[dict] | None = None
    created_at: datetime
    updated_at: datetime
