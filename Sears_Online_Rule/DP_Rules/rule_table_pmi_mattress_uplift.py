from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from functools import partial
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func
import datetime as dt

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=2002,
                         scope='div_no = 82',
                         is_active=False,
                         rule_name='pmi mattress uplift rule',
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
            return None, None

        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            return None, None

        return min_comp_rule

    def get_pre_rule(self):
        def mattress_itm_filter(row):
            if row['itm_no'] not in (17335,
17332,
17334,
17333,
17330,
17331,
17329,
17326,
17328,
17327,
17420,
17421,
17324,
17325,
15268,
15265,
15267,
15264,
17395,
17401,
17392,
17398,
17394,
17400,
17393,
17399,
17431,
17432,
17390,
17396,
17391,
17397,
18944,
18940,
18941,
18943,
18942,
18938,
18939,
15280,
15277,
15279,
15278,
15282,
15275,
15276,
15274,
15271,
15273,
15272,
15281,
15269,
15270,
17341,
17347,
17338,
17344,
17340,
17346,
17339,
17345,
17422,
17423,
17336,
17342,
17337,
17343,):
                return False
        return [
            Working_func(mattress_itm_filter, desc='Not in Mattress item list'),
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.apparel_brand_check,
            pre_rule.cost_check,
            pre_rule.pmi_check,
            pre_rule.reg_check
        ]

    def get_core_rule(self):
        return [
            core_rule.Set_to_PMI_when_PMI_exists
        ]

    def get_uplift_rule(self):
        func_handle = partial(uplift_rule._uplift_by_percentage_max, uplift=1.035)
        return [
            Working_func(func_handle, 'Mattress 3.5% uplift')
        ]
        # return []

    def get_post_rule(self):
        common_rule_lst = [
            post_rule.round_to_96,
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
