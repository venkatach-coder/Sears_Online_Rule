from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule, priority_rule
import re
from functools import partial
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func
import datetime as dt

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=2001,
                         scope='div_no = 26 and ln_no in (1,2,4)',
                         rule_name='div26 HA DP Rule',
                         rule_end_dt=dt.datetime(2019, 2, 16)
                         )

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on=['div_no', 'itm_no'], how='left')
            return df1

        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            brand = '' if row['brand'] is None else row['brand'].strip()
            product_brand = '' if row['Product_Brand'] is None else row['Product_Brand'].strip()
            if ((re.match('amana|kitchenaid|maytag|whirlpool', brand.lower()) is not None) or \
                    (re.match('amana|kitchenaid|maytag|whirlpool', product_brand.lower()) is not None)):
                return None, 'Whirlpool Exclusion'
            if row['ln_no'] != 4 and (re.match('samsung', brand.lower()) is None) and (
                    re.match('samsung', product_brand.lower()) is None):
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
            if row['ln_no'] != 4 and ((re.match('samsung', brand.lower()) is not None) or (
                    re.match('samsung', product_brand.lower()) is not None)):
                return round(row['cost_with_subsidy'] / 0.79, 2), '0.21'
            if row['ln_no'] == 4:
                return round(row['cost_with_subsidy'] / 0.65, 2), '0.35'

        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            if row['comp_name'].strip() in (
                    'Home Depot', 'Lowes', 'BestBuy'):
                return True, None
            elif row['comp_name'].strip().startswith('mkpl_'):
                return True, None
            return False, None

        return min_comp_rule

    def get_pre_rule(self):

        def div26uplift_exclusion(row):
            if row['itm_no'] in (31433,
        61433,71433,31552,61552,71552,31553,61553,71553,31632,61632,71632,31633,61633,71633,41682,81582,91582,41683,
        81583,91583,41782,81782,91782,41783,81783,91783,41072,81962,91962,41073,81963,91963,41982,81982,91982,41983,
                                 81983,91983 ):
                return False, 'Div_26 uplift exclusion'


        return [
            pre_rule.Working_func(div26uplift_exclusion, desc='Div_26 uplift exclusion'),
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.clearance_check,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check,
            pre_rule.no_kenmore
        ]

    def get_core_rule(self):
        return [
            core_rule.HA_389_399_rounding_Match_to_Min_comp_MM,
            core_rule.HA_389_399_rounding_Set_to_Min_margin_when_Min_comp_Exists,
            #core_rule.Set_to_PMI_when_PMI_exists,
        ]

    def get_uplift_rule(self):
        return [
            Working_func(partial(uplift_rule._uplift_by_percentage_max, uplift = 1.045), desc = 'HA 4.5% uplift')
        ]

    def get_post_rule(self):

        common_rule_lst = [
            post_rule.round_up_to_Map_when_close_to_map,
            post_rule.round_to_96,
            post_rule.pmi_bound_when_subsidy_exists,
            Working_func(partial(post_rule.reg_bound_PMI_func, fall_back_function=post_rule.reg_bound_d_flag),
                         desc='Revert back to PMI when dp_price is higher than reg, '
                              'send delete flag when PMI not exists')
        ]

        return [
            # Working_func(partial(post_rule.post_rule_chain,
            #                      func_lst=[post_rule.Min_PMI_DP_D_flag] + common_rule_lst)),
            Working_func(partial(post_rule.post_rule_chain,
                                 func_lst=[post_rule.DP_RECM_price] + common_rule_lst))
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        uplift_func = partial(priority_rule._priority_w_run_id, priority=500)
        return [
            Working_func(uplift_func, 'div26 500')
        ]


