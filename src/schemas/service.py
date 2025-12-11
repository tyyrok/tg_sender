from pydantic import BaseModel


class ServiceInfo(BaseModel):
    name_service: str
    version: str
