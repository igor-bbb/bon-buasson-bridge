def build_management_view(payload):

    required = ['metrics', 'context', 'navigation', 'impact', 'signal']

    for k in required:
        if k not in payload:
            return {
                "status": "error",
                "reason": f"missing {k}"
            }

    return {
        "level": payload.get("level"),
        "object": payload.get("object_name"),
        "period": payload.get("period"),

        "signal": payload.get("signal"),

        "metrics": payload.get("metrics"),
        "context": payload.get("context"),
        "impact": payload.get("impact"),

        "action": payload.get("action")
    }


def build_drilldown_management_view(payload):

    return {
        "level": payload.get("level"),
        "object": payload.get("object_name"),
        "period": payload.get("period"),
        "children_level": payload.get("children_level"),
        "items": payload.get("items", [])
    }
