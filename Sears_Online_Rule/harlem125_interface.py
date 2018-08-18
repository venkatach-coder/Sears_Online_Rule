from harlem125 import dp_rules as dpr
from typing import List, Dict, Tuple
from harlem125.exception_class import DP_Function_Definition_Err, DP_Rule_Check_Err
from harlem125 import dp_util
from functools import partial
import numpy as np
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import time, datetime as dt
import pytz
import math


class min_margin_template:
    @staticmethod
    def apply_rule(df: DataFrame, min_margin_udf):
        return df \
            .select('*',
                    min_margin_udf(
                        F.struct([df[x] for x in
                                  df.columns])
                    ).alias('min_margin')
                    ) \
            .select([c for c in df.columns] + \
                    [F.col('min_margin.value').alias('min_margin'),
                     F.col('min_margin.rule_name').alias('min_margin_rule_name')])

    @staticmethod
    def get_static_mm_udf(min_margin_mapping_input):
        def min_margin_mapping_index(row, min_margin_mapping):
            try:
                rettpl = min_margin_mapping(row)
                if type(rettpl) != tuple or len(rettpl) != 2:
                    raise DP_Function_Definition_Err('Function return value must be a tuple and length of 2')
                return rettpl
            except KeyError as e:
                raise
            except DP_Function_Definition_Err:
                raise
            except Exception as e:
                return None, str(e)

        min_margin_mapping_partial = partial(min_margin_mapping_index, min_margin_mapping=min_margin_mapping_input)
        return F.udf(min_margin_mapping_partial, returnType=T.StructType.fromJson(
            {'fields': [
                {'metadata': {}, 'name': 'value', 'nullable': True, 'type': 'double'},
                {'metadata': {}, 'name': 'rule_name', 'nullable': True, 'type': 'string'}
            ],
                'type': 'struct'}))


class min_comp_template:
    @staticmethod
    def apply_rule(df: DataFrame, min_comp_udf):
        def median(values_list):
            med = np.median(values_list)
            retval = float(med)
            if math.isnan(retval):
                return None
            return retval


        udf_median = F.udf(median, T.DoubleType())

        price_window = (Window
                        .partitionBy('div_no', 'itm_no')
                        .orderBy('price'))

        base_raw = df.select('*',
                             F.col('price').alias('min_price'),
                             F.col('comp_name').alias('min_price_comp'),
                             F.row_number().over(price_window) \
                             .alias('rn')) \
            .filter('rn == 1') \
            .drop('rn', 'price', 'comp_name')

        min_comp_all_raw = df \
            .select('div_no', 'itm_no',
                    'price', 'comp_name',
                    min_comp_udf(
                        F.struct([df[x] for x in
                                  df.columns])
                    ).alias('in_list')
                    ) \
            .filter('in_list.value').drop('in_list')

        min_comp_all = min_comp_all_raw \
            .select('div_no', 'itm_no',
                    F.col('price').alias('min_comp'),
                    F.col('comp_name').alias('min_comp_NM'),
                    F.row_number().over(price_window) \
                    .alias('rn')) \
            .filter('rn == 1') \
            .drop('rn')

        min_comp_mm = min_comp_all_raw \
            .filter('price >= min_margin') \
            .select('div_no', 'itm_no',
                    F.col('price').alias('min_comp_MM'),
                    F.col('comp_name').alias('min_comp_MM_NM'),
                    F.row_number().over(price_window) \
                    .alias('rn')) \
            .filter('rn == 1') \
            .drop('rn')
        median_df = df.groupby(['div_no', 'itm_no']) \
            .agg(udf_median(F.collect_list(F.col('price'))).alias('median_comp'))
        avg_df = min_comp_all_raw.groupby(['div_no', 'itm_no']) \
            .agg(F.avg(F.col('price')).alias('avg_comp'))

        return base_raw \
            .join(min_comp_all, on=['div_no', 'itm_no'], how='left') \
            .join(min_comp_mm, on=['div_no', 'itm_no'], how='left') \
            .join(avg_df, on=['div_no', 'itm_no'], how='left') \
            .join(median_df, on=['div_no', 'itm_no'], how='left')

    @staticmethod
    def get_min_comp_udf(min_comp_mapping_input):
        def min_comp_mpping_index(row, min_comp_mapping):
            try:
                rettpl = min_comp_mapping(row)
                if type(rettpl) != tuple or len(rettpl) != 2:
                    raise DP_Function_Definition_Err('Function return value must be a tuple and length of 2')
                return rettpl
            except KeyError as e:
                raise
            except DP_Function_Definition_Err:
                raise
            except Exception as e:
                return False, str(e)

        min_comp_mpping_index_partial = \
            partial(min_comp_mpping_index, min_comp_mapping=min_comp_mapping_input)
        return F.udf(min_comp_mpping_index_partial, returnType=T.StructType.fromJson(
            {
                'fields': [
                    {'metadata': {}, 'name': 'value', 'nullable': True, 'type': 'boolean'},
                    {'metadata': {}, 'name': 'rule_name', 'nullable': True, 'type': 'string'}
                ],
                'type': 'struct'
            }
        ))


class DP_Rule_Constructor:
    def __init__(self, rule_level, scope, rule_name, additional_source: dict = None, critical=False,
                 rule_start_dt=None, rule_end_dt=None, is_active=None,
                 reg_bound_name='delete',  # delete: 0, drop: 1, round_down:2
                 desc='', *args, **kwargs):
        self.reg_bound_code = {'delete': 0, 'drop': 1, 'round_down': 2}[reg_bound_name]
        self.reg_bound_name = reg_bound_name
        target_tbl_name = 'rule_table'
        if_exists = 'append'
        self.scope = scope
        rule_active = True if is_active is None else is_active
        inactive_reason = '' if is_active is None else 'Preset: OFF'
        time_now = (dt.datetime.now(pytz.timezone('America/Chicago'))).replace(tzinfo=None)
        self.rule_start_dt = dt.datetime(*tuple(time.gmtime(0)[:6])) if rule_start_dt is None else rule_start_dt
        rule_end_dt = dt.datetime(9999, 12, 31, 23, 59, 59) if rule_end_dt is None else rule_end_dt
        self.rule_end_dt = rule_end_dt.replace(hour=23, minute=59, second=59)  # Extend enddate to oneday
        # time check
        if rule_active is True:
            if not self.rule_start_dt < time_now < self.rule_end_dt:
                rule_active, inactive_reason = \
                    False, 'Rule inactive [{}, {}]'.format(self.rule_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                                           self.rule_end_dt.strftime('%Y-%m-%d %H:%M:%S'))

        # additional_source check
        if rule_active is True:
            if additional_source is not None:
                try:
                    assert type(additional_source) == dict, \
                        'additional_source type error, expect dict got {}'.format(type(additional_source))

                    if len(additional_source) > 0:
                        for key, item in additional_source.items():
                            assert 'table_name' in item and 'key' in item, 'additional_source format error'
                            if dp_util.tbl_exists(item['table_name']) is False:
                                raise DP_Rule_Check_Err('Table {} not found'.format(item['table_name']))
                except (DP_Rule_Check_Err, AssertionError) as e:
                    rule_active = False
                    inactive_reason = str(e)
                    if critical:
                        raise

        self.thisrule = dpr.DP_Rule(
            target_tbl_name=target_tbl_name,
            additional_source=additional_source,
            select_schema=(
                'div_no',
                'itm_no',
                'reg',                          # redundant
                'reg_bound_name',
                'reg_bound_code',
                'cost_with_subsidy',            # redundant
                'min_margin',
                'min_margin_rule_name',
                'min_comp_MM',
                'min_comp_MM_NM',
                'min_comp',
                'min_comp_NM',
                'avg_comp',
                'median_comp',
                'uplift',
                'dp_rule.pre_rule_value as pre_rule_value',
                'dp_rule.pre_rule_name as pre_rule_name',
                'dp_rule.core_rule_value as core_rule_value',
                'dp_rule.core_rule_name as core_rule_name',
                'dp_rule.uplift_rule_value as uplift_rule_value',
                'dp_rule.uplift_rule_name as uplift_rule_name',
                'dp_rule.post_rule_value as post_rule_value',
                'dp_rule.post_rule_name as post_rule_name',
                'dp_rule.deal_flag_value as deal_flag_value',
                'dp_rule.deal_flag_rule_name as deal_flag_rule_name',
                'dp_rule.day_range as day_range',
                'dp_rule.day_range_rule_name as day_range_rule_name',
                'dp_rule.priority as priority',
                'dp_rule.priority_rule_name as priority_rule_name',
                'rule_level',
                'run_id'),
            is_active=rule_active,
            inactive_rsn=inactive_reason,
            rule_level=rule_level,
            rule_name=rule_name,
            if_exists=if_exists,
            desc=desc,
            *args,
            **kwargs
        )

    @staticmethod
    def default_pre_rule(row):
        return True, 'pass'

    @staticmethod
    def default_core_rule(row):
        return None, None

    @staticmethod
    def default_uplift_rule(row):
        return row['core_rule_value'], 'No Uplift'

    @staticmethod
    def default_post_rule(row):
        return row['uplift_rule_value'], 'No post_rule'

    @staticmethod
    def defalut_deal_flag_rule(row):
        return 'N', 'Default Deal Flag 1'

    @staticmethod
    def default_price_day(row):
        if row['reg'] - row['post_rule_value'] < 0.01:
            return 0, 'Default 1-day delete_flag'
        else:
            if 10800 <= row['run_id'] < 43200:  # Morning run, push 1 day:
                day_range = 0, 'Morning run, 1 day'
            else:
                day_range = 1, 'Default 2-day pricing'

        batch_dt = dt.datetime.strptime(row['date'][:10], '%Y-%m-%d')
        daydiff = (dt.datetime.strptime(row['rule_end_date'], '%Y-%m-%d') - batch_dt).days
        if daydiff < day_range[0]:
            day_range = daydiff, 'bound by incoming dp rule ending'
        if row['uplift_end_dt'] is not None:
            daydiff = (dt.datetime.strptime(row['uplift_end_dt'], '%Y-%m-%d') - batch_dt).days
            # TODO: ignore uplift_end_dt when it will end in 2*60*60 seconds
            if daydiff < day_range[0]:
                day_range = daydiff, 'bound by incoming uplift ending'
        return day_range

    @staticmethod
    def default_priority(row):
        return int(row['run_id'] // 60), 'Default priority'

    def get_merge_func(self):
        raise Exception("NotImplementedException")

    def get_min_margin_func(self):
        raise Exception("NotImplementedException")

    def get_min_comp_func(self):
        raise Exception("NotImplementedException")

    def assemble_min_margin_func(self) -> dpr.DP_func:
        min_margin_udf = min_margin_template.get_static_mm_udf(self.get_min_margin_func())
        return dpr.DP_func(
            min_margin_template.apply_rule,
            func_desc='Apply static_table_MM udf function',
            pyudf=min_margin_udf
        )

    def assemble_min_comp_func(self) -> dpr.DP_func:
        min_comp_udf = min_comp_template.get_min_comp_udf(self.get_min_comp_func())
        return dpr.DP_func(
            min_comp_template.apply_rule,
            func_desc='Apply comp udf function',
            pyudf=min_comp_udf
        )

    def add_rule_end_date(self):
        def add_script_end_date(df: DataFrame, rule_end_date):
            return df.withColumn('rule_end_date', F.lit(rule_end_date).cast(T.StringType()))

        return add_script_end_date

    def add_reg_bound_behavior(self):
        def reg_bound_behavior(df: DataFrame, reg_bound_name, reg_bound_code):
            return df.withColumn('reg_bound_name', F.lit(reg_bound_name).cast(T.StringType())) \
                .withColumn('reg_bound_code', F.lit(reg_bound_code).cast(T.IntegerType()))

        return reg_bound_behavior

    # ---------------------------------------- #

    def get_pre_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_core_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_uplift_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_post_rule(self) -> List[dpr.Working_func]:  # Core rule price filter applied
        raise Exception("NotImplementedException")

    def get_deal_flag_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_day_range_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_priority_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_rule_func(self) -> dpr.DP_func:
        pre_rule_lst = self.get_pre_rule()
        pre_rule_lst.append(dpr.Working_func(self.default_pre_rule, 'Pass pre_rule'))

        core_rule_lst = self.get_core_rule()
        core_rule_lst.append(dpr.Working_func(self.default_core_rule, 'No core_rule'))

        uplift_rule_lst = self.get_uplift_rule()
        uplift_rule_lst.append(dpr.Working_func(self.default_uplift_rule, 'No uplift_rule'))

        post_rule_lst = self.get_post_rule()
        post_rule_lst.append(dpr.Working_func(self.default_post_rule, 'No post_rule'))

        deal_flag_rule_lst = self.get_deal_flag_rule()
        deal_flag_rule_lst.append(dpr.Working_func(self.defalut_deal_flag_rule, 'Deal_flag N'))

        day_range_rule_lst = self.get_day_range_rule()
        day_range_rule_lst.append(dpr.Working_func(self.default_price_day, 'Default 2-day pricing, 1-day Delete flag'))

        priority_rule_lst = self.get_priority_rule()
        priority_rule_lst.append(dpr.Working_func(self.default_priority, 'Default priority: 0 (minimum)'))

        return self.thisrule.rule_wrapper(pre_rule_lst, core_rule_lst, uplift_rule_lst, post_rule_lst,
                                          deal_flag_rule_lst, day_range_rule_lst, priority_rule_lst)

    def construct_rule(self) -> dpr.DP_Rule:
        self.thisrule.add_rule_layer(dpr.DP_func(self.get_merge_func(), input_type='Dict'), args=(self.scope,))
        self.thisrule.add_rule_layer(dpr.DP_func(self.add_rule_end_date()),
                                     args=(self.rule_end_dt.strftime('%Y-%m-%d'),))
        self.thisrule.add_rule_layer(self.assemble_min_margin_func())
        self.thisrule.add_rule_layer(self.assemble_min_comp_func())
        self.thisrule.add_rule_layer(dpr.DP_func(self.add_reg_bound_behavior()),
                                     args=(self.reg_bound_name, self.reg_bound_code,))
        self.thisrule.add_rule_layer(self.get_rule_func())
        return self.thisrule
