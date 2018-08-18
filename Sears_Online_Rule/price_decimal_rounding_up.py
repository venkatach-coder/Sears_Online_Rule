from typing import Dict
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']


def round_up_price(df: DataFrame, ending: float):
    ending = round(ending, 2)
    eps = 0.001
    return df.withColumn('round_up_price', F.round(F.floor(
        F.round(F.col('post_rule_value'), 2) + 1 - ending - 0.01 + eps) + ending, 2))
    # .withColumn('need_overwrite', F.when((F.col('reg') - F.col('post_rule_value') > 0.0099) &
    #                                      (F.col('reg') - F.col('round_up_price') <= 0.0099),
    #                                      F.lit(True)
    #                                      ).otherwise(F.lit(False)).cast(T.BooleanType()))




def construct_rule(ending, *args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table',
        rule_name='round up price to {:.2f}'.format(ending),
        if_exists='replace',
        desc='round up price to {:.2f}'.format(ending),
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        round_up_price,
        func_desc='round up price to {:.2f}'.format(ending),
    ), args=(ending, ))
    return thisrule
