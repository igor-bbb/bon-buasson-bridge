# VECTRA v12 — Bottom-Up Drain Layer

Base: v11.4 final clean.

Purpose:
Top-level objects can have positive total contribution and still contain real
losses inside. VECTRA must guide the user by internal drain, not only by object
performance.

Added API concepts:
- object_result_money: result of the current object as a whole vs previous year.
- internal_drain_money: sum of negative child effects only.
- drain_total follows internal_drain_money.
- vector_block.current_focus_money follows internal_drain_money.
- positive child objects are selection context and do not compensate losses.

UX logic:
- If object_result_money > 0 and internal_drain_money < 0, GPT should explain:
  "Объект в целом даёт плюс, но внутри есть управляемые потери."
- Numbers still come only from API.
- GPT may interpret the difference but may not calculate it.
