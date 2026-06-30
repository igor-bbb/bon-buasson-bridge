from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class VectraQueryRequest(BaseModel):
    message: str
    session_id: Optional[str] = 'default'

    # W15.0: Custom GPT may explicitly pass the last active Workspace state
    # back to the Action. The API never relies on hidden ChatGPT history; it
    # accepts only explicit runtime context and hydrates server-side State from it.
    active_workspace_state: Optional[Dict[str, Any]] = None
    workspace_action_map: Optional[List[Dict[str, Any]]] = None
    runtime_context: Optional[Dict[str, Any]] = None
