from typing import Optional

from pydantic import BaseModel


class VectraQueryRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
