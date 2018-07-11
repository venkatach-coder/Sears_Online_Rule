from harlem125.dp_rules import Working_func


def _clearance_check(row):
    if row['reg'] is None:
        return None, None
    temp = row['PMI'] if row['PMI'] is not None else row['reg']
    if str(round(temp, 2))[-2:] in ('87', '88', '93', '97'):
        return False, 'Clearance Block'


clearance_check = Working_func(_clearance_check, 'Clearance Check')


def _reg_check(row):
    if row['reg'] is None or row['reg'] <= 0.01:
        return False, 'Invalid reg'


reg_check = Working_func(_reg_check, 'Reg Check')


def _cost_check(row):
    if row['cost'] is None or row['cost'] <= 0.01:
        return False, 'Invalid cost'


cost_check = Working_func(_cost_check, 'Cost Check')


def _min_margin_check(row):
    if row['min_margin'] is None or row['min_margin'] <= 0.01:
        return False, 'Invalid Min_Margin'


min_margin_check = Working_func(_min_margin_check, 'Min_Margin Check')


def _ad_plan_check(row):
    if row['ad_plan'] == 'Y':
        return False, 'Ad_plan == Y'


ad_plan_check = Working_func(_ad_plan_check, 'Ad_plan Block')


def _dp_block(row):
    if row['blocked'] is not None:
        return False, 'DP Blocked'


dp_block = Working_func(_dp_block, 'DP Block_Check')


def _no_craftsman(row):
    product_brand = row['Product_Brand'] if row['Product_Brand'] is not None else ''
    brand = row['brand'] if row['brand'] is not None else ''
    if 'craftsman' in product_brand.lower() or 'craftsman' in brand.lower():
        return False, 'No Craftsman'
    if product_brand == '' and brand == '':
        return False, 'No Craftsman'

no_craftsman = Working_func(_no_craftsman, 'No Craftsman')

def _no_kenmore(row):
    pass