import pyspark.sql.functions as F
from typing import Dict
import pyspark.sql.types as T

from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def rule_name_udf(core_name, uplift_name, post_name):
    concat_lst = []
    if core_name is not None:
        concat_lst.append(core_name)
    if uplift_name is not None and uplift_name.strip() != 'No Uplift':
        concat_lst.append(uplift_name)
    if post_name is not None and post_name.strip() != 'No post_rule':
        concat_lst.append(post_name)
    return '|'.join(concat_lst)


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table_collision'] \
        .select('div_no', 'itm_no', 'final_price', 'core_rule_name', 'uplift_rule_name', 'post_rule_name')


def formatting(df: DataFrame, rule_name_func: F.udf):
    return df.withColumn('Rule_name',
                         rule_name_func(F.col('core_rule_name'), F.col('uplift_rule_name'), F.col('post_rule_name'))
                         ).drop('core_rule_name', 'uplift_rule_name', 'post_rule_name')


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rules_FTP',
        rule_name='DP RULE Table',
        desc='Rule output table formatting',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        formatting,
        func_desc='Collision FTP',
        pyudf=F.udf(rule_name_udf, T.StringType())
    ))
    return thisrule
