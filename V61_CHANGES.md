# VECTRA v6.1 package notes

Included in this package:
- management flow switched to business -> manager_top -> manager -> network -> sku
- session state extended with entry_role, entry_object_name, view_mode, filter
- management payload/view contract aligned to v6.1
- reasons payload rebuilt from DATA-based factors only
- added lightweight acceptance tests for v6.1 contract

Known gap:
- entry menu scaffolding for dedicated top-manager / manager name selection is only partially prepared; direct object entry by name works, and management drilldown flow works from business downward.


## v6.1 audit fix pack
- signal переведен на quartile distribution по margin_pre
- сигнал больше не участвует в расчете денег и не использует problem_money
- drain теперь = critical по margin_pre + finrez_pre < 0
- сортировка drain по abs(finrez_pre)
- network в management flow ведет сразу в sku
- consistency для network проверяется по sku
