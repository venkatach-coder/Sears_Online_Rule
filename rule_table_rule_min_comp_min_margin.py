from typing import Dict
from pyspark.sql import DataFrame
import dp_rules
from rule_templates import default_rules


def merge_func(df_dict: Dict[str, DataFrame], filtering_sql):
    df1 = df_dict['static_table_mm'] \
        .filter(filtering_sql) \
        .join(df_dict['min_comp_MM'],
              on=['div_no', 'itm_no'], how='left') \
        .join(df_dict['min_comp_all'],
              on=['div_no', 'itm_no'], how='left') \
        .join(df_dict['uplift_table'],
              on=['div_no', 'itm_no'], how='left')
    return df1


def core_rule(row):
    if row['pre_rule_value'] is False:
        return None, None

    if row['min_comp'] is not None:
        if row['min_comp_MM'] is not None:
            return row['min_comp_MM'], 'Match at Min_Comp_MM'
        else:
            return row['min_margin'], 'Set to Min_Margin'

    return None, 'No rule applied'


def construct_rule(rule_target_sql_str, rule_level, *args, **kwargs) -> dp_rules.DP_Rule:
    thisrule = dp_rules.DP_Rule(
        target_tbl_name='rule_table',
        rule_level=rule_level,
        rule_name='min_comp_MM_min_margin_rule',
        if_exists='append',
        desc='Matched at min_comp_MM otherwise min_margin',
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
        thisrule.core_rule_wrapper(core_rule, 'Test')
    )
    thisrule.add_rule_layer(
        thisrule.uplift_wrapper(default_rules.default_uplift, 'Default uplift')
    )
    thisrule.add_rule_layer(
        thisrule.post_rule_wrapper(default_rules.default_postrule, 'Default post_rule')
    )
    return thisrule
