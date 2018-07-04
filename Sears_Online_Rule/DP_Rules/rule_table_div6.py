from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule


class DP_Rule_div6(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=1000, rule_name='div_no = 6', if_exists='append')

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
        pre_rule_list = [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.clearance_check,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check
        ]
        return self.thisrule.pre_rule_wrapper(pre_rule_list)

    def get_core_rule(self):
        core_rule_list = [
            core_rule.Match_to_Min_comp_MM,
            core_rule.Match_to_Min_margin_when_Min_comp_Exists
        ]
        return self.thisrule.core_rule_wrapper(core_rule_list)

    def get_uplift_rule(self):
        uplift_rule_list =[
            uplift_rule.uplift_by_uplift_table
        ]
        return self.thisrule.uplift_wrapper(uplift_rule_list)

    def get_post_rule(self):
        post_rule_list = [
            post_rule.VD_Min_Reg_PMI_Upliftted_Prcie,
            post_rule.Min_Reg_PMI_Upliftted_Prcie_D_flag,
            post_rule.Reg_Bound_check_Null_when_reg_not_Exists
        ]
        return self.thisrule.post_rule_wrapper(post_rule_list)


