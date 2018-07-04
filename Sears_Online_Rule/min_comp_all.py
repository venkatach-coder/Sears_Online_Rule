from pyspark.sql.functions import udf, struct, row_number, col
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from typing import Dict


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['min_comp_all_temp']


def select_min_price(df: DataFrame):
    price_window = (Window
                    .partitionBy(df['div_no'], df['itm_no'])
                    .orderBy(df['price'].asc()))

    min_comp_df = df.select('div_no', 'itm_no',
                            col('price').alias('min_comp'),
                            col('comp_name').alias('min_comp_NM'),
                            row_number().over(price_window) \
                            .alias('rn')) \
        .filter('rn == 1') \
        .drop('rn')
    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    df.createOrReplaceTempView("tempdf")
    avg_median_df = spark.sql('''select div_no, itm_no, avg(price) as avg_comp, percentile_approx(price, 0.5) as median_comp
                             from tempdf group by div_no, itm_no''')
    return min_comp_df.join(avg_median_df, on=['div_no', 'itm_no'], how='left')


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='min_comp_all',
        rule_name='min_comp_all',
        desc='Minimum comp price (in list) in Div,item level',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        select_min_price,
        func_desc='calculate min_comp_price and get avg, median',
    ))
    return thisrule
