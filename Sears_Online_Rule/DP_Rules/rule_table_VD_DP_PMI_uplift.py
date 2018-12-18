from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from functools import partial
import pyspark.sql.functions as F
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func
import datetime as dt

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=8500,
                         scope='div_no = 8 or div_no = 96 or div_no = 52 or div_no = 4 or div_no = 44 or'
                               '(div_no = 71 and ln_no in (2, 31, 37, 50, 51, 52, 53, 54, 55,56, 59, 61, 62, 63, 64, 68, 70, 97))',
                         rule_start_dt=dt.datetime(2018,12,17),
                         rule_end_dt=dt.datetime(2018,12,25),
                         # reg_bound_behavior =   ,# drop, delete, round_down
                         rule_name='VD UPLIFT WHEN PMI EXISTS')

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
            if row['div_no'] == 8:
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
            elif row['div_no'] == 71:
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
            elif row['div_no'] == 96:
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
            elif row['div_no'] == 52:
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
            else:
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            if row['div_no'] == 8:
                if row['comp_name'].strip() in (
                        'Amazon', 'Kohls', 'Walmart', 'Bed Bath and Beyond', 'JC Penney',
                        'Target', 'Home Depot', 'Macys', 'Wayfair', 'Lowes', 'BestBuy',
                        'hhgregg', 'Nebraska Furniture Mart', 'Office Depot', 'Staples', 'ToysRUs', 'Meijer', 'Jet'):
                    return True, None
            elif row['div_no'] == 71:
                if row['comp_name'].strip() in (
                        'Home Depot', 'JC Penney', 'Amazon', 'ToysRUs', 'Walmart', 'Target', 'Jet'):
                    return True, None
            elif row['div_no'] == 96:
                if row['comp_name'].strip() in ('Amazon', 'Kohls', 'Walmart', 'Bed Bath and Beyond', 'JC Penney',
                                                'Target', 'Home Depot', 'Macys', 'Wayfair', 'Lowes', 'BestBuy',
                                                'hhgregg', 'Nebraska Furniture Mart', 'Office Depot', 'Staples',
                                                'ToysRUs', 'Meijer', 'Jet'):
                    return True, None
            elif row['div_no'] == 52:
                if row['comp_name'].strip() in (
                        'JC Penney', 'Amazon', 'ToysRUs', 'Walmart', 'Target', 'Jet'):
                    return True, None
            elif row['comp_name'].strip().startswith('mkpl_'):
                return True, None
            return False, None

        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.VD_check,
            pre_rule.pmi_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.reg_check,
        ]

    def get_core_rule(self):
        return [
            core_rule.Match_to_Min_comp_MM,
            core_rule.Set_to_PMI_when_PMI_exists
        ]

    def get_uplift_rule(self):

        func_handle = partial(uplift_rule._uplift_by_percentage_max_no_free_shipping, uplift=1.22)
        return [
            Working_func(func_handle,'22% Uplift')
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