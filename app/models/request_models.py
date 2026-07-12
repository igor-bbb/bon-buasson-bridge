from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field


class VectraQueryRequest(BaseModel):
    message: str
    session_id: Optional[str] = 'default'

    # W15.1 Runtime Context bridge for Custom GPT Actions.
    # Actions do not receive hidden dialogue/session history automatically, so
    # Custom GPT may pass the last public runtime state explicitly.
    active_workspace_state: Optional[Dict[str, Any]] = None
    workspace_action_map: Optional[List[Dict[str, Any]]] = None
    runtime_context: Optional[Dict[str, Any]] = None

    # DEV-0004 Research Runtime bridge. Custom GPT may pass the last public
    # research state explicitly because Actions do not receive hidden chat
    # state automatically.
    active_research_state: Optional[Dict[str, Any]] = None
    research_path: Optional[List[Any]] = None
    current_step: Optional[str] = None


class ResearchProgramCreateRequest(BaseModel):
    """Compact public GPT Action contract for creating a Research Program.

    Fields are optional at transport validation level so Runtime can return a
    stable structured VALIDATION_ERROR instead of FastAPI's generic 422 body.
    Semantically required fields are validated by Business Framework Research.
    """

    title: Optional[str] = Field(default=None, description='Required. Short professional title of the Research Program.')
    research_question: Optional[str] = Field(default=None, description='Required. Research question the Digital Business Analyst must answer.')
    professional_goal: Optional[str] = Field(default=None, description='Required. Professional outcome the research must achieve.')
    program_type: Optional[str] = Field(default=None, description='Required. Supported professional research program type.')
    business_domain: Optional[str] = Field(default=None, description='Required. Business Domain identifier, for example bon_buasson.')
    research_object: Optional[str] = Field(default=None, description='Optional research object or methodology being studied.')
    priority: Optional[Literal['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']] = Field(default='MEDIUM', description='Optional research priority. Defaults to MEDIUM.')
    initial_hypotheses: Optional[List[str]] = Field(default=None, description='Optional short hypothesis statements to register with the program.')
    tags: Optional[List[str]] = Field(default=None, description='Optional compact search and classification tags.')
    allow_duplicate: bool = Field(default=False, description='Allow an intentional duplicate program. Defaults to false.')
