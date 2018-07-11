from harlem125.dp_rules import Working_func


# lift_min, lift_max, uplift

def _uplift_by_uplift_table(row):
    # Will take lift_max if lift_min > lift_max
    uplift = row['uplift'] if row['uplift'] is not None else 0
    # uplift_min = row['lift_min'] if row['lift_min'] is not None else 0
    # uplift_max = row['lift_max'] if row['lift_max'] is not None else float('inf')
    # return (
    #     min(max(row['core_rule_value'] * (1 + uplift), row['core_rule_value'] + uplift_min),
    #         row['core_rule_value'] + uplift_max),
    #     'Uplift_Table: {} uplift min: {} max: {}'.format(uplift, uplift_min, uplift_max)
    # )
    return row['core_rule_value'] * (1 + uplift)

uplift_by_uplift_table = Working_func(_uplift_by_uplift_table, 'Refer to Uplift Table')
