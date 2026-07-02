from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class VectraQueryRequest(BaseModel):
    message: str
    session_id: Optional[str] = 'default'

    # W15.1 Runtime Context bridge for Custom GPT Actions.
    # Actions do not receive hidden dialogue/session history automatically, so
    # Custom GPT may pass the last public runtime state explicitly.
    active_workspace_state: Optional[Dict[str, Any]] = None
    workspace_action_map: Optional[List[Dict[str, Any]]] = None
    runtime_context: Optional[Dict[str, Any]] = None
