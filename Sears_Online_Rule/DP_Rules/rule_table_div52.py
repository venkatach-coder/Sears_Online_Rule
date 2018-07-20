from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from harlem125.dp_rules import Working_func

class Construct_DP_Rule(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=1000, rule_name='div_no = 52', if_exists='append')

    def get_merge_func(self):
        def merge_func(df_dict: Dict[str, DataFrame]):
            df1 = df_dict['temp_rule_table_base'] \
                .filter(self.thisrule.rule_name)
            return df1

        return dp_rules.DP_func(
            merge_func,
            input_type='Dict',
            func_desc='Table Selection')

    def get_pre_rule(self):
        return [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check,

        ]


    def get_core_rule(self):

        def _div52_PMI_rule(row):
            if row['PMI'] is not None:
                if 1 - row['cost_with_subsidy']/row['PMI'] >= 0.25:
                    return row['PMI'] * 0.99, '0.99 PMI'
                else:
                    return row['PMI'], 'PMI'

        div52_PMI_rule = Working_func(_div52_PMI_rule, 'Div52 PMI rule')

        return [
            core_rule.Match_to_Min_comp_MM,
            core_rule.Match_to_Min_margin_when_Min_comp_Exists,
            div52_PMI_rule
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.Min_Reg_PMI_Upliftted_Prcie_D_flag,
            post_rule.Reg_Bound_check_Null_when_reg_not_Exists
        ]
    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []

