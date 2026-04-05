from fastapi import APIRouter
from app.models.request_models import VectraQueryRequest
from app.query.orchestration import handle_query

router = APIRouter()


@router.post("/vectra/query")
def vectra_query(request: VectraQueryRequest):
    message = request.message
    session_id = getattr(request, "session_id", "default")
    period = getattr(request, "period", None)

    response = handle_query(
        message=message,
        period=period,
        session_id=session_id
    )

    return response
