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


def core_rule(row):  # Pre rule filter applied
    if row['pre_rule_value'] is False:
        return None, None

    def regular_rule(row):
        if row['min_comp'] is not None:
            if row['min_comp_MM'] is not None:
                if row['median_comp'] <= row['min_margin']: return row['min_margin'], 'min_margin, median comp'
                else: return row['min_comp_MM'], 'min_comp_MM, median comp'
            else:
                return row['min_margin'], 'Set to Min_Margin'
        return None, 'No rule apply'
    recm1 = regular_rule(row)
    def avg_shipcost(row):
        if row['avg_shipcost'] is not None:
            return row['cost_with_subsidy'] + row['avg_shipcost']
        return None, 'No rule apply'
    recm2 = avg_shipcost(row)

    if recm2[0] is None or recm1[0] is None:
        return recm1
    if recm2[0] > recm1[0]:
        return recm2
    else:
        return recm1


def post_rule(row):  # Core rule price filter applied
    def post_rule(row):  # Core rule price filter applied
        ad_plan = 'N' if row['ad_plan'] is None else row['ad_plan']
        ffm_channel = '' if row['ffm_channel'] is None else row['ffm_channel']
        PMI = math.inf if row['PMI'] is None else row['PMI']
        if 'VD' in ffm_channel \
                and ad_plan == 'D' \
                and PMI < row['min_margin']:
            return row['min_margin'], 'VD, Min_margin'
        return default_rules.postrule_min_pmi_reg_recm(row)


def construct_rule(rule_target_sql_str, rule_level, *args, **kwargs) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='div8 line rule',
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
        thisrule.core_rule_wrapper(core_rule, 'min_comp_MM, min_margin, pmi high, pmi low')
    )
    thisrule.add_rule_layer(
        thisrule.uplift_wrapper(home_uplift.uplift0305, 'Home uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(post_rule, 'VD MAP post_rule')
    )
    return thisrule
