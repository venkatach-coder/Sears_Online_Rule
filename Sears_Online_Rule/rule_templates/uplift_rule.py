from harlem125.dp_rules import Working_func




def _uplift_by_uplift_table(row):

    if row['uplift'] is not None:
        uplift = row['uplift']
        return row['core_rule_value'] * (1. + uplift), 'Refer to Uplift Table'

uplift_by_uplift_table = Working_func(_uplift_by_uplift_table, 'Refer to Uplift Table')

