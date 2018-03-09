from typing import Dict
from pyspark.sql import DataFrame
import dp_rules
from rule_templates import default_rules


def merge_func(df_dict: Dict[str, DataFrame]):
    df1 = df_dict['static_table_mm'] \
        .join(df_dict['min_comp_MM'],
              on=['div_no', 'itm_no'], how='left') \
        .join(df_dict['min_comp_all'],
              on=['div_no', 'itm_no'], how='left') \
        .join(df_dict['uplift_table'],
              on=['div_no', 'itm_no'], how='left')
    return df1


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='temp_rule_table_base',
        rule_name='rule_table_base',
        if_exists='append',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(
        dp_rules.DP_func(
            merge_func,
            input_type='Dict',
            func_desc='Table Selection')
    )
    return thisrule
