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

class Working_func_ext(dpr.Working_func):
    def __init__(self, pyfunc, desc = None):
        if desc is None:
            if type(pyfunc) == partial:
                desc = ' ----> '.join([x.desc for x in pyfunc.keywords['func_lst']])
            else:
                raise ValueError('Working_func desc cannot be empty')
        super().__init__(pyfunc, desc)

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
        max_df = min_comp_all_raw.groupby(['div_no', 'itm_no']) \
            .agg(F.max(F.col('price')).alias('max_comp'))

        return base_raw \
            .join(min_comp_all, on=['div_no', 'itm_no'], how='left') \
            .join(min_comp_mm, on=['div_no', 'itm_no'], how='left') \
            .join(avg_df, on=['div_no', 'itm_no'], how='left') \
            .join(max_df, on=['div_no', 'itm_no'], how='left') \
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

                 desc='', *args, **kwargs):

        self.sears_online_rule_schema = [
            ('pre_rule', 'boolean'),
            ('core_rule', 'double'),
            ('uplift_rule', 'double'),
            ('post_rule', 'double'),
            ('deal_flag_rule', 'string'),
            ('day_range_rule', 'integer'),
            ('priority', 'integer')
        ]  # type: List[Tuple[str,str]]

        target_tbl_name = 'rule_table'
        if_exists = 'append'
        self.scope = scope
        rule_active = True if is_active is None else is_active
        inactive_reason = '' if is_active is None else 'Preset: OFF'
        time_now = (dt.datetime.now(pytz.timezone('America/Chicago'))).replace(tzinfo=None)
        self.time_now = time_now
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

        self.thisrule = dpr.Harlem_Rule(
            target_tbl_name=target_tbl_name,
            additional_source=additional_source,
            select_schema=(
                'div_no',
                'itm_no',
                'reg',                          # redundant
                'cost_with_subsidy',            # redundant
                'min_margin',
                'min_margin_rule_name',
                'min_comp_MM',
                'min_comp_MM_NM',
                'min_comp',
                'min_comp_NM',
                'avg_comp',
                'max_comp',
                'median_comp',
                'uplift',
                'Harlem_Rule.pre_rule_value as pre_rule_value',
                'Harlem_Rule.pre_rule_name as pre_rule_name',
                'Harlem_Rule.core_rule_value as core_rule_value',
                'Harlem_Rule.core_rule_name as core_rule_name',
                'Harlem_Rule.uplift_rule_value as uplift_rule_value',
                'Harlem_Rule.uplift_rule_name as uplift_rule_name',
                'Harlem_Rule.post_rule_value as post_rule_value',
                'Harlem_Rule.post_rule_name as post_rule_name',
                'Harlem_Rule.deal_flag_rule_value as deal_flag_value',
                'Harlem_Rule.deal_flag_rule_name as deal_flag_rule_name',
                'Harlem_Rule.day_range_rule_value as day_range',
                'Harlem_Rule.day_range_rule_name as day_range_rule_name',
                'Harlem_Rule.priority_value as priority',
                'Harlem_Rule.priority_name as priority_rule_name',
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
        return round(row['uplift_rule_value'], 2), 'No post_rule'

    @staticmethod
    def defalut_deal_flag_rule(row):
        return 'N', 'Default Deal Flag 1'

    @staticmethod
    def default_price_day(row, batch_date_str):
        if row['reg'] - row['post_rule_value'] < 0.01:
            return 0, 'Default 1-day delete_flag'
        else:
            if 0 <= row['run_id'] < 43200:  # Morning run, push 1 day:
                day_range = 0, 'Morning run, 1 day'
            else:
                day_range = 1, 'Default 2-day pricing'
        batch_dt = dt.datetime.strptime(batch_date_str, '%Y-%m-%d')
        daydiff = (dt.datetime.strptime(row['rule_end_date'], '%Y-%m-%d') - batch_dt).days
        if daydiff < day_range[0]:
            day_range = daydiff, 'bound by incoming dp rule ending'
        if row['uplift_end_dt'] is not None:
            daydiff = (dt.datetime.strptime(row['uplift_end_dt'], '%Y-%m-%d') - batch_dt).days            
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
        total_func = [self.get_pre_rule(), self.get_core_rule(),
                      self.get_uplift_rule(), self.get_post_rule(),
                      self.get_deal_flag_rule(), self.get_day_range_rule(), self.get_priority_rule()]

        total_func[0].append(dpr.Working_func(self.default_pre_rule, 'Pass pre_rule'))

        total_func[1].append(dpr.Working_func(self.default_core_rule, 'No core_rule'))

        total_func[2].append(dpr.Working_func(self.default_uplift_rule, 'No uplift_rule'))

        total_func[3].append(dpr.Working_func(self.default_post_rule, 'No post_rule'))

        total_func[4].append(dpr.Working_func(self.defalut_deal_flag_rule, 'Deal_flag N'))

        total_func[5].append(dpr.Working_func(partial(self.default_price_day,
                                                           batch_date_str=self.time_now.strftime('%Y-%m-%d')),
                                                   'Default 2-day pricing, 1-day Delete flag'))
        total_func[6].append(dpr.Working_func(self.default_priority, 'Default priority: 0 (minimum)'))
        total_rule_lst = []
        for idx, each_tuple in enumerate(self.sears_online_rule_schema):
            total_rule_lst.append((each_tuple[0], each_tuple[1], total_func[idx]))
        return self.thisrule.rule_wrapper(total_rule_lst)

    def construct_rule(self) -> dpr.Harlem_Rule:
        self.thisrule.add_rule_layer(dpr.DP_func(self.get_merge_func(), input_type='Dict'), args=(self.scope,))
        self.thisrule.add_rule_layer(dpr.DP_func(self.add_rule_end_date()),
                                     args=(self.rule_end_dt.strftime('%Y-%m-%d'),))
        self.thisrule.add_rule_layer(self.assemble_min_margin_func())
        self.thisrule.add_rule_layer(self.assemble_min_comp_func())
        self.thisrule.add_rule_layer(self.get_rule_func())
        return self.thisrule
