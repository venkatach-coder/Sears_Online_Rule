import math

def uplift(row):
    if row['core_rule_value'] is not None:
        if row['core_rule_value'] <= 33:
            return math.ceil(row['core_rule_value'] * 1.2) - 0.01, '<= 33 core_rule_value * 1.2'
        if row['core_rule_value'] >= 40:
            return math.ceil(row['core_rule_value'] + 1.2) - 0.01, '>=40 core_rule_value * 1.2'
        return row['core_rule_value'], 'No uplift'

def uplift0305(row):
    '''
    Effetive for div 8,
    :param row:
    :return:
    '''
    if row['core_rule_value'] is not None:
        if row['core_rule_value'] <= 33:
            return math.ceil(row['core_rule_value'] * 1.21) - 0.01, '<= 33 core_rule_value * 1.2'
        if row['core_rule_value'] <= 36:
            return math.ceil(row['core_rule_value'] * 1.11) - 0.01, '<=36 core_rule_value * 1.11'
        if row['core_rule_value'] >= 40:
            return math.ceil(row['core_rule_value'] + 15) - 0.01, '>=40 core_rule_value + 15'
        return row['core_rule_value'], 'No uplift'

def uplift0310(row):
    if row['core_rule_value'] <= 33:
        return math.ceil(row['core_rule_value'] * 1.21) - 0.01, '<= 33 core_rule_value * 1.2'
    if row['core_rule_value'] <= 36:
        return math.ceil(row['core_rule_value'] * 1.11) - 0.01, '<=36 core_rule_value * 1.11'
    if row['core_rule_value'] >= 100:
        return math.ceil(row['core_rule_value'] + 10) - 0.01, '>=40 core_rule_value + 15'
    if 93<= row['core_rule_value'] <=98:
        return math.ceil(row['core_rule_value'] + 1) - 0.01, '[93,98] core_rule_value + 1'
    if 40<= row['core_rule_value'] <=93:
        return math.ceil(row['core_rule_value'] + 6) - 0.01, '[93,98] core_rule_value + 6'
    return row['core_rule_value'], 'No uplift'