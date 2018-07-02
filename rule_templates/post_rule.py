from dp_rules import Working_func

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
    if row['ffm_channel'] == 'VD':
        if row['ad_plan'] == 'D':
            reg = row['reg'] if row['reg'] is not None else float('inf')
            PMI = row['PMI'] if row['PMI'] is not None else float('inf')
            dp_price = row['uplift_rule_value'] if row['uplift_rule_value'] is not None else float('inf')
            if PMI < dp_price and PMI < reg:
                if PMI < row['min_margin']:
                    return row['min_margin'], 'VD, Set Price to Min_Margin'


VD_Min_Reg_PMI_Upliftted_Prcie = Working_func(_VD_Min_Reg_PMI_Upliftted_Prcie,
                                              'VD Item increase PMI to min_margin when ad_plan == D and')





