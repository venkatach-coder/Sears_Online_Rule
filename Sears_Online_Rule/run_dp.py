from harlem125 import harlem125
from Sears_Online_Rule.DP_Rules import *
from Sears_Online_Rule import *
import sys
import datetime as dt
import pytz


def run_all(target_prefix, run_id = None):
    dp_rule_lst = [x for x in sys.modules.keys() if x.startswith('Sears_Online_Rule.DP_Rules.')]
    print('------------------------------------------------')
    print(' [*] imported rules:')
    for eachmodule in dp_rule_lst:
        print(' [*] {}'.format(eachmodule))
    print('------------------------------------------------')

    time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
    if run_id is None:
        run_id = time_now.hour * 3600 + time_now.minute * 60 + time_now.second
    print(run_id)
    datetoday = time_now.strftime('%Y%m%d')
    Sears_DP = harlem125.Harlem125()
    Sears_DP.add_rule(add_run_id.construct_rule(run_id))
    Sears_DP.add_rule(uplit_table.construct_rule(time_now))
    for each_rule in dp_rule_lst:
        Sears_DP.add_rule(sys.modules[each_rule].Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(price_decimal_rounding_up.construct_rule())
    Sears_DP.add_rule(final_price_selection.construct_rule())
    Sears_DP.add_rule(dp_rule_collision.construct_rule())
    Sears_DP.add_rule(default_sales_priority_push.construct_rule())
    Sears_DP.add_rule(FTP_formatting.construct_rule())  # <------- Price Push

    # Reporting Table Generating
    # Rule_Table
    Sears_DP.add_rule(rules_FTP.construct_rule())


    # Source table loading....
    Sears_DP.load_source_table(
        {
            'static_table': {'table_name': 'dp_spark_source_tbl.static_table',
                             'key': ['div_no', 'itm_no']},
            'all_comp_all': {'table_name': 'dp_spark_source_tbl.all_comp_all',
                             'key': ['div_no', 'itm_no', 'comp_name']},
            'uplift_input': {'table_name': 'dp_spark_source_tbl.uplift_input',
                             'key': ['ID']},
        }
    )
    Sears_DP.run_all_rules()
    Sears_DP.working_table['uplift_table'] = Sears_DP.working_table['uplift_table'].cache()  # Cache rule table
    Sears_DP.working_table['rule_table'] = Sears_DP.working_table['rule_table'].cache()  # Cache rule table
    Sears_DP.working_table['rule_table_collision'] = Sears_DP.working_table['rule_table_collision'].cache()

    Sears_DP.output_working_table(
        {
            'rule_table': {'destination': 'dp_spark.{}_rule_table_{}_{:05d}'.format(target_prefix, datetoday, run_id),
                           'if_exists': 'replace'},
            'collision_FTP': {'destination': 'dp_spark.{}_collision_FTP_{}_{:05d}'.format(target_prefix, datetoday, run_id),
                              'if_exists': 'replace'},
            'rules_FTP':{'destination': 'dp_spark.{}_rules_FTP_{}_{:05d}'.format(target_prefix, datetoday, run_id),
                           'if_exists': 'replace'},
        }
    )
