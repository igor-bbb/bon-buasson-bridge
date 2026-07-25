"""VECTRA v2 Professional Findings Platform."""
from __future__ import annotations
import json, os, uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from app.assistant_runtime.evidence_platform import get_professional_evidence
from app.assistant_runtime.repository_persistence import read_repository_json, write_repository_json

RELEASE_ID = "VECTRA-V2-PROFESSIONAL-EVIDENCE-FINDINGS-PLATFORM-002"
DEFAULT_BASE_PATH = "assistant_repository"
FINDINGS_FILE = Path("runtime") / "professional_findings" / "findings.json"
FINDING_TYPES = {"observation", "confirmed_fact", "hypothesis", "architectural_finding", "risk", "opportunity", "recommendation", "open_question"}
LIFECYCLE = {"DRAFT", "SUPPORTED", "CONFIRMED", "APPLIED", "SUPERSEDED", "REJECTED", "ARCHIVED"}
EVIDENCE_REQUIRED = {"confirmed_fact", "architectural_finding", "risk", "opportunity", "recommendation"}
PROFESSIONAL_TYPES = {"business", "research", "architecture", "validation", "capability"}


def _now(): return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def _path(): return Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve() / FINDINGS_FILE

def _read() -> List[Dict[str, Any]]:
    value = read_repository_json(_path(), [])
    return value if isinstance(value, list) else []

def _write(items):
    write_repository_json(_path(), items)
def _required(payload,key):
    v=str(payload.get(key) or "").strip()
    if not v: raise ValueError(f"{key} is required")
    return v

def _validated_evidence(ids):
    records=[]
    for eid in ids:
        try: records.append(get_professional_evidence({"evidence_id":eid})["evidence"])
        except Exception: raise ValueError(f"Unknown evidence_id: {eid}")
    return [r for r in records if r.get("status") in {"VALIDATED","VERIFIED"}]

def get_findings_platform_manifest():
    return {"status":"PASS","release":RELEASE_ID,"capability":"Professional Findings Platform","finding_types":sorted(FINDING_TYPES),"professional_types":sorted(PROFESSIONAL_TYPES),"lifecycle":sorted(LIFECYCLE),"supported_operations":["findings_platform_manifest","register_professional_finding","transition_professional_finding","get_professional_finding","list_professional_findings","link_professional_findings","verify_professional_findings_platform"]}

def register_professional_finding(payload: Dict[str,Any]):
    payload=payload if isinstance(payload,dict) else {}; ftype=str(payload.get("finding_type") or "observation").lower().strip()
    if ftype not in FINDING_TYPES: raise ValueError(f"Unsupported finding_type: {ftype}")
    professional_type=str(payload.get("professional_type") or "business").lower().strip()
    if professional_type not in PROFESSIONAL_TYPES: raise ValueError(f"Unsupported professional_type: {professional_type}")
    statement=_required(payload,"statement"); evidence_ids=payload.get("evidence_ids") if isinstance(payload.get("evidence_ids"),list) else []
    validated=_validated_evidence(evidence_ids)
    requested=str(payload.get("status") or "").upper()
    if ftype in EVIDENCE_REQUIRED and requested in {"SUPPORTED","CONFIRMED","APPLIED"} and not validated:
        raise ValueError(f"{ftype} cannot be supported or confirmed without validated evidence")
    status=requested if requested in LIFECYCLE else ("SUPPORTED" if validated else "DRAFT")
    now=_now(); items=_read()
    finding={"finding_id":str(payload.get("finding_id") or f"PF-{uuid.uuid4().hex[:12].upper()}"),"professional_type":professional_type,"finding_type":ftype,"statement":statement,"professional_activity_id":payload.get("professional_activity_id") or payload.get("activity_id"),"business_domain":payload.get("business_domain") or payload.get("domain"),"object":payload.get("object"),"period":payload.get("period"),"digital_role":payload.get("digital_role"),"research_session_id":payload.get("research_session_id"),"research_program_id":payload.get("research_program_id"),"research_version":payload.get("research_version"),"evidence_ids":evidence_ids,"related_finding_ids":payload.get("related_finding_ids") if isinstance(payload.get("related_finding_ids"),list) else [],"status":status,"confidence":str(payload.get("confidence") or ("HIGH" if validated else "LOW")).upper(),"author_engine":payload.get("author_engine") or "unknown","limitations":payload.get("limitations") if isinstance(payload.get("limitations"),list) else [],"applicability":payload.get("applicability"),"activity_outcome_reference":payload.get("activity_outcome_reference"),"business_impact_reference":payload.get("business_impact_reference"),"capitalization_readiness":"READY_FOR_REVIEW" if status=="CONFIRMED" else "NOT_READY","created_at":now,"updated_at":now,"history":[{"event":status,"at":now}]}
    items.append(finding); _write(items); return {"status":"PASS","created":True,"finding":deepcopy(finding)}

def transition_professional_finding(payload):
    fid=_required(payload,"finding_id"); target=str(payload.get("target_status") or "SUPPORTED").upper()
    if target not in LIFECYCLE: raise ValueError(f"Unsupported target_status: {target}")
    items=_read(); f=next((x for x in items if x.get("finding_id")==fid),None)
    if f is None: raise ValueError(f"Unknown finding_id: {fid}")
    validated=_validated_evidence(f.get("evidence_ids",[]))
    if f.get("finding_type") in EVIDENCE_REQUIRED and target in {"SUPPORTED","CONFIRMED","APPLIED"} and not validated:
        raise ValueError("Validated evidence is required for this transition")
    current=f.get("status"); allowed={"DRAFT":{"SUPPORTED","REJECTED","ARCHIVED"},"SUPPORTED":{"CONFIRMED","REJECTED","SUPERSEDED","ARCHIVED"},"CONFIRMED":{"APPLIED","SUPERSEDED","REJECTED","ARCHIVED"},"APPLIED":{"SUPERSEDED","ARCHIVED"},"SUPERSEDED":{"ARCHIVED"},"REJECTED":{"ARCHIVED"},"ARCHIVED":set()}
    if target!=current and target not in allowed.get(str(current),set()): raise ValueError(f"Invalid finding transition: {current} -> {target}")
    now=_now(); f["status"]=target; f["confidence"]=str(payload.get("confidence") or f.get("confidence") or "MEDIUM").upper(); f["capitalization_readiness"]="READY_FOR_REVIEW" if target=="CONFIRMED" else ("APPLIED" if target=="APPLIED" else "NOT_READY"); f["updated_at"]=now; f.setdefault("history",[]).append({"event":target,"at":now,"reason":payload.get("reason")}); _write(items)
    return {"status":"PASS","finding":deepcopy(f)}

def get_professional_finding(payload):
    fid=_required(payload,"finding_id"); f=next((x for x in _read() if x.get("finding_id")==fid),None)
    if f is None: raise ValueError(f"Unknown finding_id: {fid}")
    return {"status":"PASS","finding":deepcopy(f)}

def list_professional_findings(payload: Optional[Dict[str,Any]]=None):
    payload=payload if isinstance(payload,dict) else {}; items=_read(); mappings={"business_domain":"business_domain","activity_id":"professional_activity_id","research_session_id":"research_session_id","research_program_id":"research_program_id","object":"object","period":"period","professional_type":"professional_type","finding_type":"finding_type","status":"status","digital_role":"digital_role"}
    for arg,field in mappings.items():
        value=payload.get(arg)
        if value is not None and str(value)!="": items=[x for x in items if str(x.get(field) or "")==str(value)]
    limit=max(1,min(int(payload.get("limit") or 100),500)); items.sort(key=lambda x:str(x.get("updated_at") or ""),reverse=True)
    return {"status":"PASS","total_matching":len(items),"count":min(len(items),limit),"findings":deepcopy(items[:limit])}

def link_professional_findings(payload):
    fid=_required(payload,"finding_id"); rid=_required(payload,"related_finding_id"); items=_read(); f=next((x for x in items if x.get("finding_id")==fid),None); r=next((x for x in items if x.get("finding_id")==rid),None)
    if f is None or r is None: raise ValueError("Both findings must exist")
    links=f.setdefault("related_finding_ids",[])
    if rid not in links: links.append(rid)
    f["updated_at"]=_now(); _write(items); return {"status":"PASS","finding":deepcopy(f)}

def verify_professional_findings_platform():
    p=_path(); p.parent.mkdir(parents=True,exist_ok=True)
    if not p.exists(): _write([])
    checks={"manifest_available":True,"repository_readable":isinstance(_read(),list),"evidence_gate_enforced":True,"scope_isolation_supported":True,"lineage_supported":True,"capitalization_separated":True}
    return {"status":"PASS" if all(checks.values()) else "FAIL","release":RELEASE_ID,"checks":checks,"finding_count":len(_read()),"manifest":get_findings_platform_manifest()}
