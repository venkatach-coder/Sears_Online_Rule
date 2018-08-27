from typing import Dict
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']


def final_price_selection(df: DataFrame):
    return df.withColumn('final_price', F.round(F.col('post_rule_value'), 2))


def delete_if_higher_than_reg(df: DataFrame):
    return df.withColumn('sending_delete',
                         F.when(F.col('reg') - F.col('final_price') < 0.0099,
                                True).otherwise(False).cast(T.BooleanType()))


def delete_price_offer_day_overwrite(df: DataFrame):
    df_columns = df.columns
    return df \
        .withColumnRenamed('day_range', 'old_day_range') \
        .withColumnRenamed('day_range_rule_name', 'old_day_range_rule_name') \
        .withColumn('day_range',
                    F.when((F.col('sending_delete')) & (F.col('old_day_range') > 1),
                           1).otherwise(F.col('old_day_range'))
                    ) \
        .withColumn('day_range_rule_name',
                    F.when((F.col('sending_delete')) & (F.col('old_day_range') > 1),
                           F.concat(F.col('old_day_range_rule_name'),
                                    F.lit('>delete flag, offer end date overwrite'))) \
                    .otherwise(F.col('old_day_range_rule_name'))
                    ) \
        .select(df_columns)


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table',
        rule_name='select final price',
        if_exists='replace',
        desc='Choose which column should be the final price, decide delete flag and drop price if it is higher than reg',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        final_price_selection,
        func_desc='select final price',
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        delete_if_higher_than_reg,
        func_desc='delete_if_higher_than_reg',
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        delete_price_offer_day_overwrite,
        func_desc='overwrite offer enddate if sending delete flag',
    ))
    return thisrule
