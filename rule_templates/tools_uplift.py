import math
def uplift(row):
    if row['core_rule_value'] <= 20:
        return math.ceil(row['core_rule_value'] * 1.21) - 0.01, '<= 33 core_rule_value * 1.2'
    if row['core_rule_value'] >= 100:
        return math.ceil(row['core_rule_value'] + 10) - 0.01, '>=40 core_rule_value + 10'
    if 25<= row['core_rule_value'] <=93:
        return math.ceil(row['core_rule_value'] + 6) - 0.01, '[93,98] core_rule_value + 6'
    return row['core_rule_value'], 'No uplift'