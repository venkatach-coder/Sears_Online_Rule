from harlem125.dp_rules import Working_func


def _Match_to_Min_comp_MM(row):
    if row['min_comp_MM'] is not None:
        return row['min_comp_MM'], 'Match to Min_comp_MM'


Match_to_Min_comp_MM = Working_func(_Match_to_Min_comp_MM, 'Match at Min_Comp_MM')


def _Set_to_Min_margin_when_Min_comp_Exists(row):
    if row['min_comp'] is not None:
        return row['min_margin'], 'Set to Min_Margin'


Match_to_Min_margin_when_Min_comp_Exists = Working_func(_Set_to_Min_margin_when_Min_comp_Exists,
                                                        'Set Price to Min_Margin')


def _Set_to_PMI_when_PMI_exists(row):
    if row['PMI'] is not None:
        return row['PMI'], 'Set to PMI'


Set_to_PMI_when_PMI_exists = Working_func(_Set_to_PMI_when_PMI_exists,
                                          'Set Price to PMI')


def _VD_DELETE_PMI(row):
    if row['ffm_channel'] == 'VD' and row['min_comp_MM'] is None and row['PMI'] is not None and row['reg'] is not None:
        return row['reg'], 'VD, NO COMP, PMI EXISTS, DELETE PRICE'

VD_DELETE_PMI = Working_func(_VD_DELETE_PMI, 'VD, NO COMP, PMI exists, send DELETE')

def _VD_Min_comp_MM(row):
    if row['ffm_channel'] == 'VD' and row['min_comp_MM'] is not None:
        return row['min_comp_MM'], 'VD, No PMI, Match to min_comp_MM'

VD_Min_comp_MM = Working_func(_VD_Min_comp_MM, 'VD, No PMI, Match to min_comp_MM')


def _Median_min_comp_MM_min_margin_rule(row):
    if row['min_comp'] is not None:
        if row['min_comp_MM'] is not None:
            if row['median_comp'] <= row['min_margin']:
                return row['min_margin'], 'Match at Min_margin'
            else:
                return row['min_comp_MM'], 'Match at Min_comp_MM'
        else:
            return row['min_margin'], 'Match at Min_margin'


Median_min_comp_MM_min_margin_rule = Working_func(
    _Median_min_comp_MM_min_margin_rule,
    'Min_comp_MM, Min_margin rule, Set to Min_margin if median_comp < Min_margin'
)


def _Mailable_rule(row):
    if row['ismailable'] is not None and row['avg_shipcost'] is not None:
        ret_tpl = _Median_min_comp_MM_min_margin_rule(row)
        if ret_tpl is None:
            return None
        price, rule_name = ret_tpl
        if price < row['cost_with_subsidy'] + row['avg_shipcost']:
            price = row['cost_with_subsidy'] + row['avg_shipcost']
            rule_name = 'Price at cost + avg_shipcost'
        return price, rule_name


Mailable_rule = Working_func(
    _Mailable_rule,
    'Median_min_comp_MM_min_margin_rule, cost + avg_shipcost as lower Bound when ismailalbe and avg_shipcost exists'
)


def _PMI_high_low_margin(row):
    if row['PMI'] is not None:
        if 1 - row['cost_with_subsidy'] / row['PMI'] >= 0.3:
            return row['PMI'] * 0.99, 'Unmatched, high PMI'
        else:
            return row['PMI'] * 1.02, 'Unmatched, low PMI'


PMI_high_low_margin = Working_func(
    _PMI_high_low_margin,
    '0.99 PMI when pmi_margin > 0.3 else 1.02 PMI'
)


def _HA_389_399_rounding_Match_to_Min_comp_MM(row):
    try:
        price, rule = _Match_to_Min_comp_MM(row)
    except TypeError:
        return None
    if 389 <= price <= 399:
        price = 399.0
        rule += ' Round to 399'
    return price, rule


HA_389_399_rounding_Match_to_Min_comp_MM = Working_func(
    _HA_389_399_rounding_Match_to_Min_comp_MM,
    '_Match_to_Min_comp_MM, 389-399 Rounding'
)


def _HA_389_399_rounding_Set_to_Min_margin_when_Min_comp_Exists(row):
    try:
        price, rule = _Set_to_Min_margin_when_Min_comp_Exists(row)
    except TypeError:
        return None
    if 389 <= price <= 399:
        price = 399
        rule += ' Round to 399'
    return price, rule


HA_389_399_rounding_Set_to_Min_margin_when_Min_comp_Exists = Working_func(
    _HA_389_399_rounding_Set_to_Min_margin_when_Min_comp_Exists,
    'Set_to_Min_margin_when_Min_comp_Exists, 389-399 Rounding'
)


def _Price_at_MAP(row):
    if row['MAP_price'] is not None:
        return row['MAP_price'], 'Price at MAP'


Price_at_MAP = Working_func(
    _Price_at_MAP,
    'Price at MAP'
)


def _max_min_comp_mm_map(row):
    if row['min_comp_MM'] is not None:
        if row['MAP_price'] is not None:
            map = row['MAP_price']
        else:
            map = -1.0
        return max(row['min_comp_MM'], map), 'Price at min_comp_MM MAP bounded'


max_min_comp_mm_map = Working_func(_max_min_comp_mm_map, 'price at max of map and min_comp_mm when min_comp_mm exist')


def _Match_to_Min_comp_MM_HA_instore(row):
    if row['min_comp_MM'] is not None:
        return row['min_comp_MM'], 'Branded, HA Online Min Comp'


Match_to_Min_comp_MM_HA_instore = Working_func(_Match_to_Min_comp_MM_HA_instore, 'Branded, HA Online Min Comp')


def _Match_to_ee(row):
    return row['ee_price'], row['group_name']


Match_to_ee = Working_func(_Match_to_ee, 'explore_exploit price')


def _GT80_hardlines_rule(row):
    return row['gt_price'], 'GT80 Hardlines Rule'


GT80_hardlines_rule = Working_func(_GT80_hardlines_rule, 'GT80 Hardlines Rule')


def _apparel_rtw_rule(row):
    return row['rtw_price'], 'apparel_rtw rule'


apparel_rtw_rule = Working_func(_apparel_rtw_rule, 'apparel_rtw rule')


def _PMI_uplift_2_max_5(row):
    import math
    pmi = row['PMI']
    if math.floor(round(min((1.02 * pmi), pmi + 5), 2) / 100.0) > math.floor(round(pmi, 2) / 100.0):
        return pmi, 'PMI|uplift:0.02 max 5'
    else:
        return round(min((1.02 * pmi), pmi + 5), 2), 'PMI|uplift:0.02 max 5'


PMI_uplift_2_max_5 = Working_func(_PMI_uplift_2_max_5, 'PMI|uplift:0.02 max 5')


def _PMI_uplift_1_max_5(row):
    import math
    pmi = row['PMI']
    if math.floor(round(min((1.01 * pmi), pmi + 5), 2) / 100.0) > math.floor(round(pmi, 2) / 100.0):
        return pmi, 'PMI|uplift:0.01 max 5'
    else:
        return round(min((1.01 * pmi), pmi + 5), 2), 'PMI|uplift:0.01 max 5'


PMI_uplift_1_max_5 = Working_func(_PMI_uplift_1_max_5, 'PMI|uplift:0.01 max 5')


def _send_delete_15dollar(row):
    reg = row['reg'] if row['reg'] is not None else float('inf')
    if reg <= 15.0:
        return reg

send_delete_15dollar = Working_func(_send_delete_15dollar, 'send delete for less than 15')