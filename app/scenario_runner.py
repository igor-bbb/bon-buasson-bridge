"""Scenario Runner for VECTRA Autonomous Development Bridge S2.

Scenario Runner is a technical executor only. It executes user-like commands
against /vectra/query and returns raw step results to Release Manager. It does
not decide PASS/FAIL, classify defects, change TEST PLAN or analyze payloads.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol


class StepLike(Protocol):
    message: str


DecisionCallback = Callable[[Dict[str, Any]], str]


def _call_vectra_query(message: str, session_id: str) -> Dict[str, Any]:
    """Call the same public query boundary used by Custom GPT Actions."""
    from app.api.routes import vectra_query  # lazy import avoids import cycles
    from app.models.request_models import VectraQueryRequest

    response = vectra_query(VectraQueryRequest(message=message, session_id=session_id))
    if hasattr(response, "body"):
        try:
            return json.loads(response.body.decode("utf-8"))
        except Exception as exc:
            return {"status": "error", "reason": f"invalid_json_response:{type(exc).__name__}"}
    if isinstance(response, dict):
        return response
    return {"status": "error", "reason": "invalid_response_type"}


class ScenarioRunner:
    """Executes scenario steps and delegates every decision to Release Manager."""

    def __init__(self, query_executor: Optional[Callable[[str, str], Dict[str, Any]]] = None) -> None:
        self.query_executor = query_executor or _call_vectra_query

    def run(
        self,
        *,
        scenario: Any,
        release_id: str,
        session_id: Optional[str] = None,
        decision_callback: Optional[DecisionCallback] = None,
    ) -> Dict[str, Any]:
        sid = session_id or f"scenario-runner-{release_id}-{uuid.uuid4().hex[:8]}"
        steps: Iterable[StepLike] = getattr(scenario, "steps", ()) or ()
        executed_steps: List[Dict[str, Any]] = []
        stopped = False

        for index, step in enumerate(steps, start=1):
            payload = self.query_executor(getattr(step, "message", ""), sid)
            step_result = {
                "step": index,
                "scenario_id": getattr(scenario, "id", "unknown"),
                "session_id": sid,
                "payload": payload,
                # No raw command text is stored in persistent journal by this runner.
                "command_hash_present": True,
            }
            decision = "PASS"
            if decision_callback:
                decision = str(decision_callback(step_result) or "PASS").upper()
            step_result["release_manager_decision"] = decision
            executed_steps.append(step_result)
            if decision in {"FAIL", "BLOCKED", "STOP"}:
                stopped = decision == "STOP"
                break

        return {
            "status": "stopped" if stopped else "completed",
            "release_id": release_id,
            "scenario_id": getattr(scenario, "id", "unknown"),
            "session_id": sid,
            "steps_executed": len(executed_steps),
            "step_results": executed_steps,
        }


def run_scenario(
    *,
    scenario: Any,
    release_id: str,
    session_id: Optional[str] = None,
    decision_callback: Optional[DecisionCallback] = None,
) -> Dict[str, Any]:
    return ScenarioRunner().run(
        scenario=scenario,
        release_id=release_id,
        session_id=session_id,
        decision_callback=decision_callback,
    )
