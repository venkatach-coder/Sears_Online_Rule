from Sears_Online_Rule import harlem125_points_interface as harlem
from Sears_Online_Rule.Points_Rules.points_templates import BUProgram
import random
from Sears_Online_Rule.harlem125_points_interface import Working_func_ext

class Construct_DP_Rule(harlem.DP_Points_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=1000,
                         scope='',
                         rule_name='points sample')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['full_rule_table_collision'] \
                    .join(df_dict['VBS_hierarchy'], on =['div_no'], how='left')
            return df1
        return merge_func

    def get_pre_rule(self):
        return [
        ]

    def get_core_rule(self):
        def points_randomizer(row):
            return int(random.randint(1, 10) * 10), 'Points randomizer, points test'

        return [
            Working_func_ext(points_randomizer, 'Points randomizer, points test')
        ]

    def get_post_rule(self):
        return [
        ]

    def get_points_expire_rule(self):
        def points_expire_generator(row):
            available_lenth = [7, 14, 30, 60]
            lenth = available_lenth[int(random.randint(0, 3))]
            return lenth, 'Points expire in '+str(lenth) + ' days'
        return [
            Working_func_ext(points_expire_generator, 'Points expire generator, points test')
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
