from typing import Dict
from pyspark.sql import DataFrame
import dp_rules
from rule_templates import default_rules, home_uplift
import numpy as np

def merge_func(df_dict: Dict[str, DataFrame], filtering_sql):
    df1 = df_dict['temp_rule_table_base'] \
        .filter(filtering_sql)
    return df1


def pre_rule(row):
    if row['MAP_price'] is None:
        return False, 'No MAP'
    return default_rules.default_prerule(row)


def core_rule(row):
    if row['MAP_price'] is not None: return row['MAP_price'], 'MAP_price'
    if row['PMI'] is not None: return row['PMI'], 'PMI'
    return None, 'No rule applied'


def post_rule(row):  # Core rule price filter applied
    if row['uplift_rule_value'] is not None:
        return row['uplift_rule_value'], 'MAP price'


def construct_rule(rule_target_sql_str, rule_level, *args, **kwargs) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='div14 rule',
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
        thisrule.pre_rule_wrapper(pre_rule, 'Default pre_rule')
    )
    thisrule.add_rule_layer(
        thisrule.core_rule_wrapper(core_rule, 'min_comp_MM, min_margin')
    )
    thisrule.add_rule_layer(
        thisrule.uplift_wrapper(home_uplift.uplift0310, 'No uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(post_rule, 'VD MAP post_rule')
    )
    return thisrule
