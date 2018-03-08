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
    if row['PMI'] is not None:
        return row['PMI'], 'PMI'
    return None, 'No rule applied'


def uplift(row):
    if row['core_rule_value'] <= 10: return math.ceil(row['PMI'] * 1.3), '1.30PMI'
    if row['core_rule_value'] <= 40: return math.ceil(row['PMI'] * 1.2), '1.20PMI'
    if row['core_rule_value'] <= 43: return math.ceil(row['PMI'] * 1.12), '1.12PMI'
    if row['core_rule_value'] > 50: return math.ceil(row['PMI'] * 1.05), '1.05PMI'
    return row['PMI'], 'No uplift'


def construct_rule(rule_target_sql_str, rule_level) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='Apparel rule',
        if_exists='append',
        desc=rule_target_sql_str
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
        thisrule.uplift_wrapper(uplift, 'Apparel Uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(default_rules.default_prerule, 'Default Postrule')
    )
    return thisrule
