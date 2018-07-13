from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from harlem125.dp_rules import Working_func

class Construct_DP_Rule(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=1000,
                         rule_name='div_no = 34', if_exists='append')

    def get_merge_func(self):
        def merge_func(df_dict: Dict[str, DataFrame]):
            df1 = df_dict['temp_rule_table_base'] \
                .filter(self.thisrule.rule_name) \
            .join(df_dict['electrical_whitelist'], on= ['div_no', 'itm_no'], how='left') \
            .join(df_dict['electrical_multipliers'], on= ['div_no', 'itm_no'], how='left')
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
            pre_rule.reg_check,
        ]


    def get_core_rule(self):

        def _ce_rule(row):
            if row['tools_min_price'] is not None and row['min_comp_MM'] is not None:
                return row['min_comp_MM'] * (1+row['tools_min_price']), 'Set to min_comp_MM'
            elif row['PMI'] is not None and row['tools_pmi_price'] is not None:
                return row['PMI'] * (1+row['tools_pmi_price']), 'Set to PMI price'
            elif row['tools_pmi_price'] is not None and row['reg'] is not None:
                return row['reg'] * (1+row['tools_pmi_price']),'Set to Reg price'

        ce_rule = Working_func(_ce_rule, 'CE special rules with multiplier')
        return [
            ce_rule
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.VD_Min_Reg_PMI_Upliftted_Prcie,
            post_rule.Min_Reg_PMI_Upliftted_Prcie_D_flag,
            post_rule.Reg_Bound_check_Null_when_reg_not_Exists
        ]

    def get_deal_flag_rule(self):
        return []