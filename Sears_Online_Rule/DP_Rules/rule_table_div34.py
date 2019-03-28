from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from functools import partial
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func


class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        additional_tbl = {
            'electrical_whitelist': {
                'table_name': 'dp_spark_source_tbl.Electrical_Whitelist',
                'key': ['div_no', 'itm_no']},
            'electrical_multipliers': {
                'table_name': 'dp_spark_source_tbl.electrical_multipliers',
                'key': ['div_no', 'itm_no']},
        }
        super().__init__(rule_level=1000, additional_source=additional_tbl,
                         scope='div_no = 34', rule_name='div34')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['electrical_whitelist'], on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['electrical_multipliers'], on=['div_no', 'itm_no'], how='left')
            return df1

        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'

        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            if row['comp_name'].strip() in (
                    'Home Depot', 'Lowes', 'Jet'):
                return True, None
            elif row['comp_name'].strip().startswith('mkpl_'):
                return True, None
            return False, None

        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.reg_check,
        ]

    def get_core_rule(self):

        def _ce_rule(row):
            if row['tools_min_price'] is not None and row['min_comp_MM'] is not None:
                return row['min_comp_MM'] * (1 + row['tools_min_price']), 'Set to min_comp_MM'
            elif row['PMI'] is not None and row['tools_pmi_price'] is not None:
                return row['PMI'] * (1 + row['tools_pmi_price']), 'Set to PMI price'
            elif row['tools_pmi_price'] is not None and row['reg'] is not None:
                return row['reg'] * (1 + row['tools_pmi_price']), 'Set to Reg price'

        ce_rule = Working_func(_ce_rule, 'CE special rules with multiplier')
        return [
            ce_rule
        ]

    def get_uplift_rule(self):
        return [

        ]

    def get_post_rule(self):
        common_rule_lst = [
            post_rule.round_to_96,
            post_rule.map_lower_bound,
            post_rule.reg_bound_d_flag]

        return [
            # Working_func(partial(post_rule.post_rule_chain,
            #                      func_lst=[post_rule.VD_Increase_PMI_to_min_margin] + common_rule_lst
            #                      )),
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
        return []
