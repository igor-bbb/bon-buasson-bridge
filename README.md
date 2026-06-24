# VECTRA v1.3 — BUG-025 Product Business Opening Fix

Status: COMPLETED

Included fixes:
- Product Layer voice request without a complete parent context opens business-level Product View.
- Example: `Покажи Вода 2026-02` opens `Бизнес → Вода`.
- Contextual Product Layer opening is preserved inside a full branch.
- Example inside `Бизнес → Труш → Головченко → Оптторг-15`: `Покажи Вода 2026-02` opens `... → Оптторг-15 → Вода`.
- Explicit business scope is supported: `Покажи Вода по бизнесу 2026-02`.
- Object opening has priority over Voice Rating when a concrete object and role are present.
- Example: `Покажи Труш Максим как топ-менеджера 2026-02` opens the object screen, not a rating.
