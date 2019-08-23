from harlem125.dp_rules import Working_func
from typing import List
import math


def post_rule_chain(row: dict, func_lst: List[Working_func]):
    ret_rule_name_lst = []
    rowcopy = row.copy()
    val = None
    for each_func in func_lst:
        ret_val_raw = each_func.pyfunc(rowcopy)
        if ret_val_raw is not None and ret_val_raw != (None, None):
            val = ret_val_raw[0] if type(ret_val_raw) == tuple else ret_val_raw
            rsn = ret_val_raw[1] if type(ret_val_raw) == tuple else each_func.desc
            ret_rule_name_lst.append(rsn)
            if val is None:
                break
            rowcopy['uplift_rule_value'] = val
        else:
            break
    total_rsn = '>'.join([x.strip() for x in ret_rule_name_lst if x != '']) if len(ret_rule_name_lst) > 0 else None
    return val, total_rsn


def _Min_PMI_DP_D_flag(row):
    if row['ad_plan'] == 'D' and row['PMI'] is not None:
        PMI = row['PMI']
        dp_price = row['uplift_rule_value'] if row['uplift_rule_value'] is not None else float('inf')
        if PMI <= dp_price:
            return None, 'Set Price to PMI, drop price'
        else:
            return dp_price, 'Set Price to DP_price'


Min_PMI_DP_D_flag = Working_func(_Min_PMI_DP_D_flag,
                                  'Take Minimum price of PMI, dp_price, when Ad_plan == D and PMI exists')


def _DP_RECM_price(row):
    return row['uplift_rule_value'], 'Set Price to DP_price'

DP_RECM_price = Working_func(_DP_RECM_price, 'DP recommendation price')


def _VD_Increase_PMI_to_min_margin(row):
    if row['ffm_channel'] == 'VD' and row['ad_plan'] == 'D':
        PMI = row['PMI'] if row['PMI'] is not None else float('inf')
        dp_price = row['uplift_rule_value']
        if PMI <= dp_price:
            if PMI <= round(row['min_margin'], 2):
                return round(row['min_margin'], 2), 'VD Set Price to Min_Margin'
            else:
                return None, 'PMI is higher than min_margin, drop price'



VD_Increase_PMI_to_min_margin = Working_func(
    _VD_Increase_PMI_to_min_margin,
    'VD Item increase PMI to min_margin when ad_plan == D and ffm_channel == VD')




def _round_up_to_Map_when_close_to_map(row):
    if row['MAP_price'] is not None:
        map = row['MAP_price']
        if 0.99 * map <= row['uplift_rule_value'] < map:
            return map, 'Round up to map'
        else:
            return row['uplift_rule_value'], ''
    else:
        return row['uplift_rule_value'], ''

round_up_to_Map_when_close_to_map = Working_func(
    _round_up_to_Map_when_close_to_map,
    'Round up to map when close to map'
)


def _uplift_to_MAP_when_below(row):
    if row['MAP_price'] is not None:
        map = row['MAP_price']
        if row['uplift_rule_value'] < map:
            return map, 'Uplift to map'
        else:
            return row['uplift_rule_value'], ''
    else:
        return row['uplift_rule_value'], ''

uplift_to_MAP_when_below = Working_func(
    _uplift_to_MAP_when_below,
    'Uplift to map when below map'
)


def _reg_bound_d_flag(row):
    reg = row['reg'] if row['reg'] is not None else float('inf')
    dp_price = row['uplift_rule_value']
    if dp_price <= reg - 0.01:
        return dp_price, ''
    else:
        return reg, 'Send Delete Flag'

reg_bound_d_flag = Working_func(_reg_bound_d_flag, 'reg_bound_delete_flag')


def _reg_bound_drop(row):
    reg = row['reg'] if row['reg'] is not None else float('inf')
    dp_price = row['uplift_rule_value']
    if dp_price <= reg - 0.01:
        return dp_price, ''
    else:
        return None, 'Drop Price'

reg_bound_drop = Working_func(_reg_bound_drop, 'reg_bound_drop')


def reg_bound_PMI_func(row, fall_back_function: Working_func):
    if row['PMI'] is not None:
        reg = row['reg'] if row['reg'] is not None else float('inf')
        dp_price = row['uplift_rule_value']
        if dp_price <= reg - 0.01:
            return dp_price, ''
        else:
            return row['PMI'], 'Rever to PMI Drop Price'
    else:
        return fall_back_function.pyfunc(row)


def _price_round_to_96(row):
    if row['reg'] - row['uplift_rule_value'] > 0.005:
        return math.floor(round(row['uplift_rule_value'], 2)) + 0.96
    return row['uplift_rule_value'], '' # no .96 rounding when at reg

round_to_96 = Working_func(_price_round_to_96, 'Round to .96')

def _price_round_to_80(row):
    return math.floor(round(row['uplift_rule_value'], 2)) + 0.80

round_to_80 = Working_func(_price_round_to_80, 'Round to .80')


def _price_round_to_00(row):
    return math.ceil(round(row['uplift_rule_value'], 2))

round_to_00 = Working_func(_price_round_to_00, 'Round to .00')

def _cost_check(row):
    if row['uplift_rule_value'] >= row['cost_with_subsidy']:
        return row['uplift_rule_value'], ''
    elif row['uplift_rule_value'] < row['cost_with_subsidy']:
        return None, 'Drop price because of cost check'

cost_check = Working_func(_cost_check, 'if less than cost then drop')


def _map_check(row):
    if row['MAP_price'] is not None and row['uplift_rule_value'] < float(row['MAP_price']):
        return None, 'Drop price because of less than MAP'
    else:
        return row['uplift_rule_value'], ''

map_check = Working_func(_map_check, 'Drop price because of less than MAP')


def _check_mkpl(row):
    if row['mkpl_price'] is not None:
        if row['uplift_rule_value'] > row['mkpl_price']:
            return row['mkpl_price'], 'back to mkpl'
        return row['uplift_rule_value'], ''
    else:
        return row['uplift_rule_value'], ''

check_mkpl = Working_func(_check_mkpl, 'if less than mkpl then mkpl')


def _min_margin_lb(row):
    if row['uplift_rule_value'] < row['min_margin']:
        return row['min_margin'], 'min_margin bound'
    return row['uplift_rule_value'], ''
min_margin_lb = Working_func(_min_margin_lb, 'min_margin check')

def _pmi_bound_when_subsidy_exists(row):
    if row['override_cost'] is not None and row['PMI'] is not None:
        if row['uplift_rule_value'] > row['PMI']:
            return row['PMI'], 'PMI bound when subsidy exists'
    return row['uplift_rule_value'], ''

pmi_bound_when_subsidy_exists = Working_func(_pmi_bound_when_subsidy_exists, 'PMI subsidy bound')

def _map_lower_bound(row):
    if row['MAP_price'] is not None and row['uplift_rule_value'] < float(row['MAP_price']):
        return row['MAP_price'], 'Priced at MAP'
    return row['uplift_rule_value'], ''

map_lower_bound = Working_func(_map_lower_bound, 'Make sure post price is not lower than MAP')

def _map_lower_bound_pmi_check(row):
    if row['MAP_price'] is not None and row['uplift_rule_value'] < float(row['MAP_price']):
        if row['PMI'] is not None and row['PMI'] <= row['MAP_price']:
            return row['PMI'], 'PMI < MAP, set to PMI'
        else:
            return row['MAP_price'], 'SET TO MAP'
    return row['uplift_rule_value'], ''
map_lower_bound_pmi_check = Working_func(_map_lower_bound_pmi_check, 'MAP PMI check')


def _pmi_bound(row):
    if row['PMI'] is None:
        return row['uplift_rule_value'], ''
    else:
        if row['uplift_rule_value'] > row['PMI']:
            return row['PMI'], 'PMI bound'
        else:
            return row['uplift_rule_value'], ''

pmi_bound = Working_func(_pmi_bound, 'PMI Bound')

