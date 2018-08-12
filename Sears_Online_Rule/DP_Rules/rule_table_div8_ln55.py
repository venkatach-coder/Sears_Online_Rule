from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule


class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        additional_tbl = {'mailable_table': {
            'table_name': 'dp_spark_source_tbl.mailable_table',  # required
            'key': ['div_no', 'itm_no'],  # required
        }}
        super().__init__(rule_level=2000, additional_source=additional_tbl,
                         scope='div_no = 8 and ln_no in (55)',
                         rule_name='div8_ln55 HOME Regular DP Rule')


    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no','itm_no','price','comp_name'),
                      on = ['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on = ['div_no', 'itm_no'], how='left') \
                .join(df_dict['mailable_table'], on=['div_no', 'itm_no'], how='left')
            return df1
        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            if row['comp_name'].strip() in (
                    'Amazon', 'Kohls', 'Walmart', 'Bed Bath and Beyond', 'JC Penney',
                    'Target', 'Home Depot', 'Macys', 'Wayfair', 'Lowes', 'BestBuy',
                    'hhgregg', 'Nebraska Furniture Mart', 'Office Depot', 'Staples', 'ToysRUs', 'Meijer', 'Jet'):
                return True, None
            elif row['comp_name'].strip().startswith('mkpl_'):
                return True, None
            return False, None
        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.min_comp_check,
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check
        ]

    def get_core_rule(self):
        return [
            core_rule.Mailable_rule,
            core_rule.Median_min_comp_MM_min_margin_rule
        ]

    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.VD_Min_Reg_PMI_Upliftted_Prcie,
            post_rule.Min_Reg_PMI_Upliftted_Prcie_D_flag,
            post_rule.Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []
