from typing import Dict
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']


def final_price_selection(df: DataFrame):
    return df.withColumn('final_price', F.col('round_up_price'))


def drop_if_higher_than_reg(df: DataFrame):
    return df.withColumn('drop_price',
                         F.when((F.col('reg') - F.col('final_price') < 0.0099) & (F.col('reg_bound_code') == 1),
                                True).otherwise(False).cast(T.BooleanType())) \
        .withColumn('new_final_price',
                    F.when(F.col('drop_price'), F.lit(None)).otherwise(F.col('final_price')).cast(T.DoubleType())) \
        .withColumn('new_post_rule_name', F.when(F.col('drop_price'),
                                                 F.concat(F.col('post_rule_name'),
                                                          F.lit('|equal or higher than reg, drop'))) \
                    .otherwise(F.col('post_rule_name')).cast(T.StringType())) \
        .select([F.col('new_post_rule_name').alias('post_rule_name') if c == 'post_rule_name' else \
                     F.col('new_final_price').alias('final_price') if c == 'final_price' else \
                         c for c in df.columns])


def rounddown_if_higher_than_reg(df: DataFrame, ending):
    retdf = df.withColumn('rounddown_price',
                          F.when((F.col('reg') - F.col('final_price') < 0.0099) & (F.col('reg_bound_code') == 2),
                                 True).otherwise(False).cast(T.BooleanType()))

    if ending is None:
        retdf = retdf.withColumn('adjusted_price',
                                 F.when(F.col('rounddown_price'),
                                        F.round(F.col('reg') - 0.01, 2)) \
                                 .otherwise(F.col('final_price'))) \
            .withColumn('new_post_rule_name', F.when(F.col('rounddown_price'),
                                                     F.concat(F.col('post_rule_name'),
                                                              F.lit('|equal or higher than reg, rounding down 1 cent'))
                                                     ).otherwise(F.col('post_rule_name')).cast(T.StringType()))

    else:
        ending = round(ending, 2)
        eps = 0.001
        retdf = retdf.withColumn('adjusted_price',
                                 F.when(F.col('rounddown_price'),
                                        F.round(F.floor(F.round(F.col('reg'), 2) - ending + eps) + ending, 2)) \
                                 .otherwise(F.col('final_price'))) \
            .withColumn('new_post_rule_name',
                        F.when(F.col('rounddown_price'),
                               F.concat(F.col('post_rule_name'),
                                        F.lit('|equal or higher than reg, rounding down to nearest {:.2f}'.format(
                                            ending)
                                        ))
                               ).otherwise(F.col('post_rule_name')).cast(T.StringType())
                        )

    return retdf.select([F.col('new_post_rule_name').alias('post_rule_name') if c == 'post_rule_name' else \
                             F.col('adjusted_price').alias('final_price') if c == 'final_price' else \
                                 c for c in df.columns])


def delete_if_higher_than_reg(df: DataFrame):
    retdf = df.withColumn('is_delete_flag',
                          F.when((F.col('reg') - F.col('final_price') < 0.0099) & (F.col('reg_bound_code') == 0),
                                 True).otherwise(False).cast(T.BooleanType())) \
        .withColumn('sending_delete',
                    F.when(F.col('is_delete_flag'),
                           True).otherwise(False).cast(T.BooleanType())) \
        .withColumn('new_post_rule_name', F.when(F.col('is_delete_flag'),
                                                 F.concat(F.col('post_rule_name'),
                                                          F.lit('|equal or higher than reg, sending_delete_flag'))
                                                 ).otherwise(F.col('post_rule_name')).cast(T.StringType()))

    return retdf.select([F.col('new_post_rule_name').alias('post_rule_name') if c == 'post_rule_name' else \
                             c for c in df.columns] + ['sending_delete'])


def construct_rule(ending=None, *args, **kwargs) -> dp_rules.DP_Rule_base:
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
        drop_if_higher_than_reg,
        func_desc='drop_if_higher_than_reg',
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        rounddown_if_higher_than_reg,
        func_desc='rounddown_if_higher_than_reg',
    ), args=(ending,))
    thisrule.add_rule_layer(dp_rules.DP_func(
        delete_if_higher_than_reg,
        func_desc='delete_if_higher_than_reg',
    ))
    return thisrule
