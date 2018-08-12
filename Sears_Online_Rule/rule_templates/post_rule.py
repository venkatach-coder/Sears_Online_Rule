from harlem125.dp_rules import Working_func


def _Min_Reg_PMI_Upliftted_Prcie_D_flag(row):
    if row['ad_plan'] == 'D':
        reg = row['reg'] if row['reg'] is not None else float('inf')
        PMI = row['PMI'] if row['PMI'] is not None else float('inf')
        dp_price = row['uplift_rule_value'] if row['uplift_rule_value'] is not None else float('inf')
        if PMI <= dp_price and PMI <= reg:
            return None, 'Set Price to PMI'
        elif dp_price < reg and dp_price < PMI:
            return dp_price, 'Set Price to DP_price'
        else:
            return reg, 'Send Delete Flag'


Min_Reg_PMI_Upliftted_Prcie_D_flag = Working_func(_Min_Reg_PMI_Upliftted_Prcie_D_flag,
                                                  'Take Minimum price of reg, PMI, dp_price, when Ad_plan == D')


def _Reg_Bound_check_Null_when_reg_not_Exists(row):
    if row['reg'] is None:
        return None, 'Reg not Exists'
    if row['uplift_rule_value'] < row['reg']:
        return row['uplift_rule_value'], 'Set Price to DP_price'
    else:
        return row['reg'], 'Send Delete Flag'


Reg_Bound_check_Null_when_reg_not_Exists = Working_func(_Reg_Bound_check_Null_when_reg_not_Exists,
                                                        'Regular price Bound check, Reg price must exist')


def _VD_Min_Reg_PMI_Upliftted_Prcie(row):
    if row['ffm_channel'] == 'VD' and row['ad_plan'] == 'D':
        reg = row['reg'] if row['reg'] is not None else float('inf')
        PMI = row['PMI'] if row['PMI'] is not None else float('inf')
        dp_price = row['uplift_rule_value']
        if dp_price < PMI and dp_price < reg:
            return row['uplift_rule_value'], 'Set Price to DP_price'
        elif PMI <= dp_price and PMI <= reg:
            if PMI <= round(row['min_margin'], 2):
                return round(row['min_margin'], 2), 'VD Set Price to Min_Margin'
            else:
                return None, 'PMI is higher than min_margin'
        else:
            return row['reg'], 'Send Delete Flag'


VD_Min_Reg_PMI_Upliftted_Prcie = Working_func(
    _VD_Min_Reg_PMI_Upliftted_Prcie,
    'VD Item increase PMI to min_margin when ad_plan == D and ffm_channel == VD and PMI <= min(DP_price, reg)')


def _Reg_Upliftted_Prcie(row):
    reg = row['reg'] if row['reg'] is not None else float('inf')
    dp_price = row['uplift_rule_value']
    if dp_price < reg:
        return row['uplift_rule_value'], 'Set Price to DP_price'
    else:
        return row['reg'], 'Send Delete Flag'


Reg_Upliftted_Prcie = Working_func(
    _Reg_Upliftted_Prcie,
    ' Item send minimum in reg or dp_price')


def _Round_to_Map_Min_Reg_PMI_Upliftted_Prcie_D_flag(row):
    rettpl = _Min_Reg_PMI_Upliftted_Prcie_D_flag(row)
    if rettpl is None or rettpl[0] is None:
        return rettpl
    price, rule_name = rettpl
    map = row['MAP_price'] if row['MAP_price'] is not None else float(
        '-inf')  # here, we DO NOT want to use map price if it is null
    if price < map:
        price = map
        rule_name += ' | Round to MAP_price'
    return price, rule_name


Round_to_MAP_Min_Reg_PMI_Upliftted_Prcie_D_flag = Working_func(
    _Round_to_Map_Min_Reg_PMI_Upliftted_Prcie_D_flag,
    'Take Minimum price of reg, PMI, dp_price, when Ad_plan == D, MAP_price as lower Bound'
)


def _Round_to_MAP_VD_Min_Reg_PMI_Upliftted_Prcie(row):
    rettpl = _VD_Min_Reg_PMI_Upliftted_Prcie(row)
    if rettpl is None or rettpl[0] is None:
        return rettpl
    price, rule_name = rettpl
    map = row['MAP_price'] if row['MAP_price'] is not None else float(
        '-inf')  # here, we DO NOT want to use map price if it is null
    if price < map:
        price = map
        rule_name += ' | Round to MAP_price'
    return price, rule_name


Round_to_MAP_VD_Min_Reg_PMI_Upliftted_Prcie = Working_func(
    _Round_to_MAP_VD_Min_Reg_PMI_Upliftted_Prcie,
    'VD Item increase PMI to min_margin when ad_plan == D and ffm_channel == VD and PMI <= min(DP_price, reg), MAP_price as lower Bound'
)


def _Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists(row):
    rettpl = _Reg_Bound_check_Null_when_reg_not_Exists(row)
    if rettpl is None or rettpl[0] is None:
        return rettpl
    price, rule_name = rettpl
    map = row['MAP_price'] if row['MAP_price'] is not None else float(
        '-inf')  # here, we DO NOT want to use map price if it is null
    if price < map:
        price = map
        rule_name += ' | Round to MAP_price'
    return price, rule_name


Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists = Working_func(
    _Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists,
    'Regular price Bound check, Reg price must exist, MAP price as lower Bound'
)


def _Round_to_MAP_HA_reg_bound(row):
    map = row['MAP_price'] if row['MAP_price'] is not None else float('-inf')
    if 0.99 * map <= row['uplift_rule_value'] < map:
        return map, 'Round to MAP'
    elif row['uplift_rule_value'] >= row['reg'] - 0.01:
        return row['reg'] - 0.01, 'Set to reg - 0.01'
    else:
        return row['uplift_rule_value'], 'Keep on core rule price'


Round_to_MAP_HA_reg_bound = Working_func(
    _Round_to_MAP_HA_reg_bound,
    '0.99 MAP Rounding with reg bound'
)


def _Round_to_MAP_HA_reg_bound_D_flag(row):
    if row['ad_plan'] == 'D' and row['PMI'] is not None and row['PMI'] < row['uplift_rule_value']:
        return row['PMI'], 'Delete DP, Set to Original Promo'


Round_to_MAP_HA_reg_bound_D_flag = Working_func(
    _Round_to_MAP_HA_reg_bound_D_flag,
    '0.99 MAP Rounding with reg bound when ad_plan == D, sending PMI when dp_price > PMI'
)


def _Round_to_MAP_HA_reg_PMI_bound_no_adplan(row):
    reg = row['reg'] if row['reg'] is not None else float('inf')
    dp_price = row['uplift_rule_value']
    if dp_price < reg:
        map = row['MAP_price'] if row['MAP_price'] is not None else float('-inf')
        if 0.99 * map <= row['uplift_rule_value'] < map:
            return map, 'Round to MAP'
        else:
            return row['uplift_rule_value'], 'Keep on core rule price'
    elif dp_price >= reg:
        if row['PMI'] is not None:
            return row['PMI'], 'Delete DP, Set to Original Promo'
        else:
            return row['reg'], 'Send Delete Flag'


Round_to_MAP_HA_reg_PMI_bound_no_adplan = Working_func(
    _Round_to_MAP_HA_reg_PMI_bound_no_adplan,
    'when no adplan considered, send map or dp_price when dp_price < reg, else reg or pmi'
)


def _reg_bound(row):
    reg = row['reg'] if row['reg'] is not None else float('inf')
    dp_price = row['uplift_rule_value']
    if dp_price <= reg - 0.01:
        return dp_price, 'Recom_prc'
    else:
        return reg, 'Send Delete Flag'


reg_bound = Working_func(_reg_bound, 'reg_bound')
