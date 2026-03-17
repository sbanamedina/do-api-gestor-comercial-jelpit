# models.py
from pydantic import BaseModel


class NitRequest(BaseModel):
    nit: str
