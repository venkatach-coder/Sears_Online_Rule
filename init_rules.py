import os

def init_dp_rules():
    rule_folder = os.path.join(os.path.dirname(__file__), 'Sears_Online_Rule', 'DP_Rules')
    rule_module_lst = os.listdir(os.path.join(os.path.dirname(__file__), 'Sears_Online_Rule', 'DP_Rules'))
    rule_lst = [x[:-3] for x in rule_module_lst \
            if x.endswith('.py') and x != '__init__.py' and x.startswith('rule_table')]
    with open(os.path.join(rule_folder, '__init__.py'), 'w') as f:
        if len(rule_lst) > 0:
            f.write('__all__ = {}'.format(str(rule_lst)))
        else:
            f.write('')
    init_points_rules()

def init_points_rules():
    rule_folder = os.path.join(os.path.dirname(__file__), 'Sears_Online_Rule', 'Points_Rules')
    rule_module_lst = os.listdir(os.path.join(os.path.dirname(__file__), 'Sears_Online_Rule', 'Points_Rules'))
    rule_lst = [x[:-3] for x in rule_module_lst \
                if x.endswith('.py') and x != '__init__.py' and x.startswith('points_table')]
    with open(os.path.join(rule_folder, '__init__.py'), 'w') as f:
        if len(rule_lst) > 0:
            f.write('__all__ = {}'.format(str(rule_lst)))
        else:
            f.write('')


if __name__ == '__main__':
    init_dp_rules()