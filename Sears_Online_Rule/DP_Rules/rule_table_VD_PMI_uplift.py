from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from functools import partial
import pyspark.sql.functions as F
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func
import datetime as dt

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=601,
                         scope='(div_no = 8 and ln_no in (1,21,41, 16,19,20,21,23,24,27,30,99, 55)) or div_no = 14 or div_no = 24 or div_no = 96 or div_no = 44',
                         rule_start_dt=dt.datetime(2018,12,15),
                         rule_end_dt=dt.datetime(2018,12,25),
                         # reg_bound_behavior =   ,# drop, delete, round_down
                         rule_name='VD UPLIFT')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name') \
                  .filter('trim(comp_name) like "mkpl_%"') \
                  .groupBy(['div_no', 'itm_no']) \
                  .agg(F.min(F.col('price')).alias('mkpl_price')),
                  on=['div_no', 'itm_no'], how='left')
            return df1

        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            return None, None

        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            return None, None
        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.VD_check,
            pre_rule.pmi_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.reg_check,
            pre_rule.div_8_no_VD,
            pre_rule.no_TW
        ]

    def get_core_rule(self):
        return [
            core_rule.Set_to_PMI_when_PMI_exists
        ]

    def get_uplift_rule(self):
        def VD_uplift(row):
            if row['ffm_channel'] == 'VD':
                if row['div_no'] == 44:
                    return uplift_rule._uplift_by_percentage_max(row, 1.20)[0], 'VD 20% UPLIFT'
                else:
                    return uplift_rule._uplift_by_percentage_max(row, 1.27)[0], 'VD 27% UPLIFT'

        return [
            Working_func(VD_uplift, 'VD UPLIFT'),
        ]

    def get_post_rule(self):
        common_rule_lst = [post_rule.round_to_96,
                           post_rule.check_mkpl,
                           post_rule.min_margin_lb,
                           post_rule.uplift_to_MAP_when_below,
                           post_rule.reg_bound_d_flag]

        return [
            Working_func(partial(post_rule.post_rule_chain,
                                 func_lst=[post_rule.DP_RECM_price] + common_rule_lst))
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []