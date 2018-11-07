from Sears_Online_Rule import harlem125_points_interface as harlem
from Sears_Online_Rule.Points_Rules.points_templates import BUProgram
import random
from Sears_Online_Rule.harlem125_points_interface import Working_func_ext

class Construct_DP_Rule(harlem.DP_Points_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=1000,
                         scope='div_no = 8 and itm_no in (1153, 1159, 1169, 1177, 1210, 1213, 1231, 1241, 1271, 1281)',
                         rule_name='points test')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['full_rule_table_collision'].filter(scope) \
                    .join(df_dict['VBS_hierarchy'], on =['div_no'], how='left')
            return df1
        return merge_func

    def get_pre_rule(self):
        return [
        ]

    def get_core_rule(self):
        def points_10(row):
            return int(10), 'points test, 10 points'

        return [
             Working_func_ext(points_10, 'points test, 10 points')
        ]

    def get_post_rule(self):
        return [
        ]

    def get_points_expire_rule(self):
        def expire_14d(row):
            return 14, 'Points expire in '+str(14) + ' days'
        return [
            Working_func_ext(expire_14d, 'points test, 14d expire')
        ]

    def get_points_end_date_rule(self):
        return []

    def get_action_rule(self):
        return []

    def get_BUProgram_rule(self):
        return [
            BUProgram.buprogram_Multi_BU
        ]

    def get_ExpenseAllocation_rule(self):
        return []

    def get_MEMBER_STATUS(self):
        return []
