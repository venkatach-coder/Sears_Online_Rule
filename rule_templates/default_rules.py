__all__ = ['default_uplift', 'default_prerule', 'default_postrule']
def default_uplift(row):
    '''
    Default uplift, Using uplift table
    :param row: Run by pyspark, individual row of a table
    :return:
    '''
    if row['uplift'] is not None and row['core_rule_value'] is not None:
        return row['core_rule_value'] * (1 + row['uplift']), 'Uplift_table'
    else:
        return row['core_rule_value'], None

def default_clearance_prerule(row):
    if row['reg'] is None:
        return False, None
    temp = row['PMI'] if row['PMI'] is not None else row['reg']
    if str(round(temp,2))[-2:] in ('87','88','93','97'):
        return False, 'Clearance'
    return default_prerule(row)

def default_prerule(row):
    '''
    Default prerule filtering, consier: reg, cost, min_margin, ad_plan, blocked
    :param row: Run by pyspark, individual row of a table
    :return:
    '''
    if row['reg'] is None or row['reg'] < 0.01:
        return False, 'Invalid reg'

    if row['cost'] is None or row['cost'] < 0.01 or \
                    row['cost_with_subsidy'] is None or row['cost_with_subsidy'] < 0.01:
        return False, 'Invalid cost'

    if row['min_margin'] is None:
        return False, 'Invalid min_margin'

    if row['ad_plan'] == 'Y':
        return False, 'Ad_plan == Y'

    if row['blocked'] is not None:
        return False, 'Blocked flag'
    return True, None

def default_corerule(row):
    '''
    default core_rule: min_comp, min_comp_MM, PMI
    :param row:
    :return:
    '''
    if row['min_comp'] is not None:
        if row['min_comp_MM'] is not None:
            return row['min_comp_MM'], 'Match at Min_Comp_MM'
        else:
            return row['min_margin'], 'Set to Min_Margin'
    else:
        if row['PMI'] is not None:
            return row['PMI'], 'Set price to PMI'
    return None, 'No rule applied'


def default_postrule(row):
    '''
    Default Post rule checking (Reg check)
    :param row:
    :return:
    '''
    if row['uplift_rule_value'] is not None:
        return_val = round(row['uplift_rule_value'], 2)
        if return_val >= row['reg']:
            return row['reg'] - 0.01, 'Set to reg'
        else:
            return return_val, 'DP Price'

def postrule_min_pmi_reg_recm(row):
    import math
    '''
    ad_plan == 'D': return min(upfted_price, reg, pmi)
    ad_plan != 'D': return min(upfted_price, reg)
    :param row:
    :return:
    '''
    ad_plan = 'N' if row['ad_plan'] is None else row['ad_plan']
    PMI = math.inf if row['PMI'] is None else row['PMI']
    def argsort(seq):
        return sorted(range(len(seq)), key=seq.__getitem__)
    if ad_plan == 'D':
        prclst = [row['uplift_rule_value'], PMI, row['reg']]
        prc_rule_lst = ['DP Price', 'Set to PMI', 'Set to reg']
        rank = argsort(prclst)
        return prclst[rank[0]], prc_rule_lst[rank[0]]
    else:
        prclst = [row['uplift_rule_value'], row['reg']]
        prc_rule_lst = ['DP Price', 'Set to reg']
        rank = argsort(prclst)
        return prclst[rank[0]], prc_rule_lst[rank[0]]
