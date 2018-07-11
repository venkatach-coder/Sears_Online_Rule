from harlem125.dp_rules import Working_func


def _Min_Reg_PMI_Upliftted_Prcie_D_flag(row):
    if row['ad_plan'] == 'D':
        reg = row['reg'] if row['reg'] is not None else float('inf')
        PMI = row['PMI'] if row['PMI'] is not None else float('inf')
        dp_price = row['uplift_rule_value'] if row['uplift_rule_value'] is not None else float('inf')
        if PMI < dp_price and PMI < reg:
            return PMI, 'Set Price to PMI'
        elif dp_price < reg and dp_price < PMI:
            return dp_price, 'Set Price to DP_price'
        else:
            return reg, 'Set Price to reg'


Min_Reg_PMI_Upliftted_Prcie_D_flag = Working_func(_Min_Reg_PMI_Upliftted_Prcie_D_flag,
                                                  'Take Minimum price of reg, PMI, dp_price, when Ad_plan == D')


def _Reg_Bound_check_Null_when_reg_not_Exists(row):
    if row['reg'] is None:
        return None, 'Reg not Exists'
    if row['uplift_rule_value'] <= row['reg']:
        return row['uplift_rule_value'], 'Set Price to DP_price'
    else:
        return row['reg'], 'Set Price to reg'


Reg_Bound_check_Null_when_reg_not_Exists = Working_func(_Reg_Bound_check_Null_when_reg_not_Exists,
                                                        'Regular price Bound check, Reg price must exist')


def _VD_Min_Reg_PMI_Upliftted_Prcie(row):
    if row['ffm_channel'] == 'VD' and row['ad_plan'] == 'D':
        reg = row['reg'] if row['reg'] is not None else float('inf')
        PMI = row['PMI'] if row['PMI'] is not None else float('inf')
        dp_price = row['uplift_rule_value'] if row['uplift_rule_value'] is not None else float('inf')
        if PMI < dp_price and PMI < reg:
            if PMI < row['min_margin']:
                return row['min_margin'], 'VD Set Price to Min_Margin'
            else:
                return None, 'PMI is higher than min_margin'


VD_Min_Reg_PMI_Upliftted_Prcie = Working_func(
    _VD_Min_Reg_PMI_Upliftted_Prcie,
    'VD Item increase PMI to min_margin when ad_plan == D and ffm_channel == VD and PMI <= min(DP_price, reg)')


def _Round_to_Map_Min_Reg_PMI_Upliftted_Prcie_D_flag(row):
    rettpl = _Min_Reg_PMI_Upliftted_Prcie_D_flag(row)
    if rettpl is None or rettpl[0] is None:
        return rettpl
    price, rule_name = rettpl
    if price < row['MAP_price']:
        price = row['MAP_price']
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
    if price < row['MAP_price']:
        price = row['MAP_price']
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
    if price < row['MAP_price']:
        price = row['MAP_price']
        rule_name += ' | Round to MAP_price'
    return price, rule_name


Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists = Working_func(
    _Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists,
    'Regular price Bound check, Reg price must exist, MAP price as lower Bound'
)


def _Round_to_MAP_HA_reg_bound(row):
    if 0.99 * row['MAP_price'] < row['uplift_rule_value'] <= row['MAP_price']:
        return row['MAP_price'], 'Round to MAP'
    elif row['uplift_rule_value'] >= row['reg'] - 0.01:
        return row['reg'], 'Set to reg'
    else:
        return row['uplift_rule_value'], 'Keep on core rule price'


Round_to_MAP_HA_reg_bound = Working_func(
    _Round_to_MAP_HA_reg_bound,
    '0.99 MAP Rounding with reg bound'
)


def _Round_to_MAP_HA_reg_bound_D_flag(row):
    if row['ad_plan'] == 'D' and row['PMI'] is not None:
        if row['PMI'] > row['ad_plan']:
            return _Round_to_MAP_HA_reg_bound(row)
        elif row['PMI'] < row['reg']:
            return row['PMI'], 'Delete DP, Set to Original Promo'
        else:
            return row['reg'], 'Set to reg'


Round_to_MAP_HA_reg_bound_D_flag = Working_func(
    _Round_to_MAP_HA_reg_bound_D_flag,
    '0.99 MAP Rounding with reg bound when ad_plan == D, sending PMI when dp_price > PMI'
)
