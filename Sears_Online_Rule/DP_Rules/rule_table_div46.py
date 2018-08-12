from Sears_Online_Rule import harlem125_interface as harlem
from harlem125.dp_rules import Working_func
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
import re

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=1000,
                         scope='div_no = 46 and ln_no in (1,2,4,51,5)', rule_name='div46')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no','itm_no','price','comp_name'),
                      on = ['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on = ['div_no', 'itm_no'], how='left')
            return df1
        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            brand = '' if row['brand'] is None else row['brand']
            product_brand = '' if row['Product_Brand'] is None else row['Product_Brand']
            if ((re.match('amana|kitchenaid|maytag|whirlpool', brand.lower()) is not None) or \
                     (re.match('amana|kitchenaid|maytag|whirlpool', product_brand.lower()) is not None)):
                return None, 'Whirlpool Exclusion'
            if (re.match('samsung', brand.lower()) is None) and (re.match('samsung', product_brand.lower()) is None):
                return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
            if ((re.match('samsung', brand.lower()) is not None) or (re.match('samsung', product_brand.lower()) is not None)):
                return round(row['cost_with_subsidy'] / 0.79, 2), '0.21'
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
        return [
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
            core_rule.HA_389_399_rounding_Set_to_Min_margin_when_Min_comp_Exists
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.Round_to_MAP_HA_reg_PMI_bound_no_adplan
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return [
            Working_func(lambda x: 500, 'div26 500')
        ]