from pydantic import BaseModel


class VectraQueryRequest(BaseModel):
    message: str
