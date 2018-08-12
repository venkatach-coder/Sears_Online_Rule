from typing import Dict
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table_collision']\
            .join(work_df['static_table'].select('div_no','itm_no','sales_30'),
                  on=['div_no','itm_no'],
                  how = 'left')


def priority_calc(df: DataFrame):
    total_window = Window.partitionBy()
    sales_window = Window.partitionBy().orderBy('sales_30')
    return df.withColumn('sales_priority',F.dense_rank().over(sales_window)) \
    .withColumn('max_priority', F.max('sales_priority').over(total_window)) \
    .withColumn('priority_index',
                F.col('priority') + ((F.col('sales_priority') / F.col('max_priority')) * 1000).cast(T.IntegerType()))\
    .withColumn('new_priority_rule', F.concat(F.col('priority_rule_name'), F.lit(' + sales_priority')))\
    .select([x for x in df.columns if x not in ('priority', 'priority_rule_name')] + \
            ['priority_index', 'new_priority_rule']) \
    .withColumnRenamed('priority_index','priority') \
    .withColumnRenamed('new_priority_rule', 'priority_rule_name') \
    .select([x for x in df.columns]) \
    .drop('sales_30')


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table_collision',
        rule_name='sales priority',
        if_exists='replace',
        desc='Base Priority by Sales 30',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        priority_calc,
        func_desc='',
    ))
    return thisrule
