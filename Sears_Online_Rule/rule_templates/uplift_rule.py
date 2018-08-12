from harlem125.dp_rules import Working_func


def _uplift_by_uplift_table(row):
    if row['uplift'] is not None:
        uplift = row['uplift']
        return row['core_rule_value'] * (1. + uplift), 'Uplift: {:.2f}'.format(row['uplift'])


uplift_by_uplift_table = Working_func(_uplift_by_uplift_table, 'Refer to Uplift Table')


def _PMI_uplift_with_max(row, uplift, max_val):
    import math

    if math.floor(round(min((uplift * row['core_rule_value']), row['core_rule_value'] + max_val), 2) / 100.0) > \
            math.floor(round(row['core_rule_value'], 2) / 100.0):
        return row['core_rule_value'], 'PMI'
    else:
        return round(min((uplift * row['core_rule_value']), row['core_rule_value'] + max_val), 2), \
               'PMI|uplift:{:.2f} max {:.2f}'.format(uplift - 1, round(max_val, 2))

