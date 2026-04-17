
VECTRA Backend

Current contract for summary endpoints:
{
  context,
  metrics,
  drain_block,
  goal,
  focus_block,
  navigation
}

Rules:
- DATA is source of truth
- finrez_final only for business
- gap only in percents
- drain sorted by potential_money DESC
- no analysis_block / signal / legacy summary fields in summary response
