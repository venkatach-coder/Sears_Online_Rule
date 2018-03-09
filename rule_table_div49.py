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
    if row['min_comp_MM'] is not None:
        return row['min_comp_MM'] * 0.99, 'min_comp_MM * 0.99'

def post_rule(row):  # Core rule price filter applied
    return default_rules.postrule_min_pmi_reg_recm(row)


def construct_rule(rule_target_sql_str, rule_level, *args, **kwargs) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='div 49 Division',
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
        thisrule.pre_rule_wrapper(default_rules.default_prerule, 'Default pre_rule')
    )
    thisrule.add_rule_layer(
        thisrule.core_rule_wrapper(core_rule, 'min_comp_MM * .99')
    )
    thisrule.add_rule_layer(
        thisrule.uplift_wrapper(default_rules.default_uplift, 'Default uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(post_rule, 'Default postrule')
    )

    return thisrule
