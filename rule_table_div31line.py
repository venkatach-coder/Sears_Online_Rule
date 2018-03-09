from typing import Dict
from pyspark.sql import DataFrame
import dp_rules
from rule_templates import default_rules, home_uplift
import numpy as np
import math

def merge_func(df_dict: Dict[str, DataFrame], filtering_sql):
    df1 = df_dict['temp_rule_table_base'] \
        .filter(filtering_sql)
    return df1


def core_rule(row):
    if row['PMI'] is not None: return row['PMI'], 'PMI uplift'
    return None, 'No rule applied'

def uplift_rule(row):
    PMI = row['PMI']
    if PMI * 1.2 >= max(row['cost_with_subsidy']/0.9, row['cost_with_subsidy']+3):
        return math.floor(PMI*1.2), '1.2*PMI'
    if row['cost_with_subsidy']/0.9 > row['cost_with_subsidy']+3:
        return math.floor(row['cost_with_subsidy']/0.9), 'COST / 0.9'
    else:
        return row['cost_with_subsidy'] + 3, 'COST + 3'


def construct_rule(rule_target_sql_str, rule_level, *args, **kwargs) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='div 31 line 8',
        if_exists='append',
        desc=rule_target_sql_str,
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(
        dp_rules.DP_func(
            merge_func,
            input_type='Dict',
            func_desc='Table Selection'),
        args=(rule_target_sql_str,)
    )
    thisrule.add_rule_layer(
        thisrule.pre_rule_wrapper(default_rules.default_clearance_prerule, 'Default pre_rule')
    )
    thisrule.add_rule_layer(
        thisrule.core_rule_wrapper(core_rule, 'PMI')
    )
    thisrule.add_rule_layer(
        thisrule.uplift_wrapper(uplift_rule, 'custom uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(default_rules.default_postrule, 'Default Postrule')
    )
    return thisrule
