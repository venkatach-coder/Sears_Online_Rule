from harlem125.dp_rules import Working_func
import math


def _uplift_by_uplift_table(row):
    rule_name_lst = []
    if row['uplift'] is not None or row['lift_min'] is not None:
        uplift = 0.0 if row['uplift'] is None else row['uplift']
        lift_min = 0.0 if row['lift_min'] is None else row['lift_min']
        lift_max = float('inf') if row['lift_max'] is None else row['lift_max']
        price = min(max(row['core_rule_value'] * (1. + uplift), row['core_rule_value'] + lift_min),
                    row['core_rule_value'] + lift_max)
        if uplift > 0.00999:
            rule_name_lst.append('{:.1f}%'.format(uplift * 100))
        if lift_min > 0.00999:
            rule_name_lst.append('Min: ${:.2f}'.format(lift_min))
        if not math.isinf(lift_max):
            rule_name_lst.append('Max: ${:.2f}'.format(lift_max))
        return price, 'uplift ' + ' '.join(rule_name_lst)


uplift_by_uplift_table = Working_func(_uplift_by_uplift_table, 'Refer to Uplift Table')


def _PMI_uplift_with_max(row, uplift, max_val):  # Will not touch anything 99,99, 199.99,299.99 etc
    import math

    if math.floor(round(min((uplift * row['core_rule_value']), row['core_rule_value'] + max_val), 2) / 100.0) > \
            math.floor(round(row['core_rule_value'], 2) / 100.0):
        return row['core_rule_value'], 'PMI'
    else:
        return round(min((uplift * row['core_rule_value']), row['core_rule_value'] + max_val), 2), \
               'PMI uplift:{:.1f}%'.format((uplift - 1.0) * 100.0) + '' if math.isinf(max_val) \
                   else ' max {:.2f}'.format(uplift - 1, round(max_val, 2))


def _uplift_by_percentage_max(row, uplift, max_val):
    return round(min((uplift * row['core_rule_value']), row['core_rule_value'] + max_val), 2), \
               'PMI uplift:{:.1f}%'.format((uplift - 1.0) * 100.0) + '' if math.isinf(max_val) \
                   else ' max {:.2f}'.format(uplift - 1, round(max_val, 2))

def _uplift_those_with_subsidy(row):
    if math.fabs(row['cost_with_subsidy'] - row['cost']) < 0.00999:
        return row['core_rule_value']*1.04, 'uplift 4% for items with no subsidy'


uplift_those_with_subsidy = Working_func(_uplift_those_with_subsidy, 'for those no subsidy uplift 0.04')