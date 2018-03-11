from typing import Dict
from pyspark.sql import DataFrame
import dp_rules
from rule_templates import default_rules, tools_uplift
import numpy as np
import math


def merge_func(df_dict: Dict[str, DataFrame], filtering_sql):
    df1 = df_dict['temp_rule_table_base'] \
        .filter(filtering_sql) \
        .join(df_dict['electrical_whitelist'],
              on=['div_no', 'itm_no'], how='inner') \
        .join(df_dict['electrical_multipliers'] \
              .select('div_no', 'itm_no', 'tools_pmi_price', 'tools_min_price'),
              on=['div_no', 'itm_no'], how='left')
    return df1


def core_rule(row):
    if row['tools_min_price'] is not None and row['min_comp_MM'] is not None:
        return row['min_comp_MM'] * (1 + row['tools_min_price']), 'Min_comp_MM'
    if row['tools_pmi_price'] is not None and row['PMI'] is not None:
        return row['PMI'] * (1 + row['tools_pmi_price']), 'PMI'
    if row['tools_pmi_price'] is not None:
        return row['reg'] * (1 + row['tools_pmi_price']), 'Reg'


def post_rule(row):  # Core rule price filter applied
    ad_plan = 'N' if row['ad_plan'] is None else row['ad_plan']
    ffm_channel = '' if row['ffm_channel'] is None else row['ffm_channel']
    PMI = math.inf if row['PMI'] is None else row['PMI']
    if 'VD' in ffm_channel \
            and ad_plan == 'D' \
            and PMI < row['min_margin']:
        return row['min_margin'], 'VD, Min_margin'
    return default_rules.postrule_min_pmi_reg_recm(row)


def clean_output(df):
    return df.drop('tools_pmi_price', 'tools_min_price')


def construct_rule(rule_target_sql_str, rule_level, *args, **kwargs) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='div 34 Division',
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
        thisrule.core_rule_wrapper(core_rule, 'min_comp_MM, PMI, reg')
    )
    thisrule.add_rule_layer(
        thisrule.uplift_wrapper(tools_uplift.uplift, 'Default uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(post_rule, 'VD Postrule')
    )
    thisrule.add_rule_layer(
        dp_rules.DP_func(clean_output)
    )
    return thisrule
