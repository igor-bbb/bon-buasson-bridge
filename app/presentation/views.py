def _build_drain_rows(payload):
    items = payload.get("all_items") or []

    prepared = []

    for item in items:
        gap = ((item.get("impact") or {}).get("gap_loss_money")) or 0.0

        prepared.append({
            "object_name": item.get("object_name"),
            "gap": gap,
        })

    # 🔴 СОРТИРОВКА ПО ДЕНЬГАМ
    prepared.sort(key=lambda x: x["gap"], reverse=True)

    return prepared[:5]
