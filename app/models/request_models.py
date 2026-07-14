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


class BusinessRuntimeAccessVerificationRequest(BaseModel):
    """Compact public contract for Stage 1 Business Runtime access verification."""

    period: Optional[str] = Field(default=None, description='Optional business period. Defaults to latest available period.')
    limit_per_level: int = Field(default=3, ge=1, le=10, description='Maximum number of discovered sample objects per professional level.')


class BusinessResearchExecutionStartRequest(BaseModel):
    research_question: Optional[str] = Field(default=None, description='Required professional research question.')
    professional_goal: Optional[str] = Field(default=None, description='Required professional goal of the research.')
    business_domain: Optional[str] = Field(default=None, description='Required Business Domain identifier.')
    professional_hypothesis: Optional[str] = Field(default=None, description='Optional active professional hypothesis. Defaults to the research question.')
    title: Optional[str] = Field(default=None, description='Optional short title of the research execution.')
    program_type: Optional[str] = Field(default='business_framework_research', description='Optional Research Program type.')
    research_object: Optional[str] = Field(default='Existing Business Framework', description='Optional object of research.')
    priority: Optional[Literal['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']] = Field(default='HIGH')
    period: Optional[str] = Field(default=None, description='Optional business period.')
    research_program_id: Optional[str] = Field(default=None, description='Optional existing Research Program id.')
    open_questions: Optional[List[str]] = Field(default=None, description='Optional initial open questions.')
    tags: Optional[List[str]] = Field(default=None, description='Optional compact tags.')
    allow_duplicate: bool = Field(default=False)


class BusinessResearchTaskExecuteRequest(BaseModel):
    research_execution_id: str = Field(description='Research Execution identifier.')
    task_id: Optional[str] = Field(default=None, description='Optional Research Task id. Next recommended task is used when omitted.')
    object_id: Optional[str] = Field(default=None, description='Optional direct Business object identifier.')
    period: Optional[str] = Field(default=None, description='Optional business period override.')


class BusinessResearchFindingRequest(BaseModel):
    research_execution_id: str = Field(description='Research Execution identifier.')
    task_id: str = Field(description='Research Task that produced the Finding.')
    statement: str = Field(description='Professional Finding statement.')
    finding_type: Optional[Literal['observation','confirmed_fact','hypothesis','architectural_finding','risk','opportunity','recommendation','open_question']] = Field(default='confirmed_fact')
    evidence_ids: Optional[List[str]] = Field(default=None, description='Validated Evidence ids. Task evidence is used when omitted.')
    confidence: Optional[Literal['LOW','MEDIUM','HIGH','VERIFIED']] = Field(default='HIGH')
    business_impact: Optional[str] = Field(default=None)
    recommendation: Optional[str] = Field(default=None)
    limitations: Optional[List[str]] = Field(default=None)
    object: Optional[str] = Field(default=None)



class BusinessObjectDiscoveryRequest(BaseModel):
    """Scalable read-only contract for Business Object Discovery v2."""

    object_type: Optional[str] = Field(default=None, description='Optional object type: business, top_manager, manager, network, category, tmc_group or sku. Required for object listing.')
    period: Optional[str] = Field(default=None, description='Optional period filter. Latest period remains available in metadata when omitted.')
    name_contains: Optional[str] = Field(default=None, description='Optional case-insensitive display-name search.')
    offset: int = Field(default=0, ge=0, description='Pagination offset for the requested object type.')
    limit: int = Field(default=50, ge=1, le=100, description='Maximum objects returned in one page.')
    sort_by: str = Field(default='default_business_order', description='Sorting: name, priority or default_business_order.')
    summary_only: bool = Field(default=False, description='Return counts and available types without object records.')


class ResearchWorkspaceSnapshotRequest(BaseModel):
    """Compact public read-only request for a complete Workspace research snapshot."""

    workspace_id: Optional[str] = Field(default=None, description='Existing Business Workspace identifier.')
    object_id: Optional[str] = Field(default=None, description='Stable public Business object identifier returned by Business Object Discovery.')
    business_domain: Optional[str] = Field(default=None, description='Optional Business Domain selector when workspace_id is omitted.')
    business_object: Optional[str] = Field(default=None, description='Optional managed business object selector when workspace_id is omitted.')
    period: Optional[str] = Field(default=None, description='Optional business period selector when workspace_id is omitted.')
    object_type: Optional[str] = Field(default=None, description='Optional professional object type for snapshot identity.')


class BusinessDecisionFrameworkValidationRequest(BaseModel):
    """Compact public contract for Stage 3 Decision Framework validation.

    Optional scenarios allow Laboratory to override the default role scenarios
    without exposing internal Runtime structures.
    """

    business_domain: Optional[str] = Field(default='bon_buasson', description='Business Domain identifier.')
    period: Optional[str] = Field(default=None, description='Optional business period. Latest available period is used when omitted.')
    research_question: Optional[str] = Field(default=None, description='Optional Stage 3 research question.')
    professional_goal: Optional[str] = Field(default=None, description='Optional professional goal for the validation activity.')
    runtime_session_id: Optional[str] = Field(default=None, description='Optional stable Runtime session id.')
    decision_scenarios: Optional[List[Dict[str, Any]]] = Field(default=None, description='Optional compact decision scenarios. Default scenarios are generated from available Business objects.')


class BusinessDecisionFrameworkValidationReportRequest(BaseModel):
    report_id: Optional[str] = Field(default=None, description='Optional report id. Latest report is returned when omitted.')

class BusinessResearchExecutionReferenceRequest(BaseModel):
    research_execution_id: str = Field(description='Research Execution identifier.')
    reason: Optional[str] = Field(default=None, description='Optional pause or resume reason.')
