from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query
from app.domain.data_quality import inspect_personnel_cost_support


def test_entity_dictionary_built_from_data(app_with_sample_data):
    payload = get_entity_dictionary('2026-02')
    assert 'manager_top' in payload
    assert 'National A' in payload['manager_top']['canonical']
    assert payload['network']['index']['varus'] == 'VARUS'
    assert payload['tmc_group']['index']['лимонады 2л'] == 'Лимонады 2л'


def test_business_drilldown_and_next_step(app_with_sample_data):
    summary = orchestrate_vectra_query('бизнес февраль 2026')
    assert summary['status'] == 'ok'
    assert summary['data']['action']['next_step'] == 'спуститься до топ-менеджеров или менеджеров'

    drill = orchestrate_vectra_query('бизнес февраль 2026 сети')
    assert drill['status'] == 'ok'
    assert drill['data']['children_level'] == 'manager_top'


def test_category_and_tmc_group_comparison(app_with_sample_data):
    category_cmp = orchestrate_vectra_query('сравни категорию напитки 2л февраль 2026 к февраль 2026')
    assert category_cmp['status'] == 'ok'
    assert category_cmp['query']['level'] == 'category'

    group_cmp = orchestrate_vectra_query('сравни группу тмц лимонады 2л февраль 2026 к февраль 2026')
    assert group_cmp['status'] == 'ok'
    assert group_cmp['query']['level'] == 'tmc_group'


def test_personnel_cost_status_endpoint_logic(app_with_sample_data):
    result = inspect_personnel_cost_support()
    assert result['status'] in {'found', 'header_only', 'not_found', 'no_data'}


def test_normalization_skips_total_rows_and_keeps_business_field():
    from app.domain.normalization import normalize_row

    total_row = normalize_row({
        'period': '2026-02',
        'business': 'Bon',
        'manager_top': 'Total',
        'manager': 'Total',
        'network': 'Total',
        'category': 'Total',
        'tmc_group': 'Total',
        'sku': 'Total',
        'revenue': '100',
        'finrez_pre': '10',
        'markup': '5',
        'margin_pre': '2',
    })
    assert total_row is None

    regular_row = normalize_row({
        'period': '2026-02',
        'business': 'Bon',
        'manager_top': 'Без менеджера',
        'manager': 'Без менеджера',
        'network': 'VARUS',
        'category': 'Напитки',
        'tmc_group': 'Лимонады',
        'sku': 'SKU 1',
        'revenue': '100',
        'finrez_pre': '10',
        'finrez': '8',
        'markup': '5',
        'margin_pre': '2',
        'personnel_cost': '3',
    })
    assert regular_row is not None
    assert regular_row['business'] == 'Bon'
    assert regular_row['manager_top'] == 'Без менеджера'
    assert regular_row['finrez'] == 8.0
    assert regular_row['personnel_cost'] == 3.0
