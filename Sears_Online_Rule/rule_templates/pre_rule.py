from harlem125.dp_rules import Working_func
import re


def _min_comp_check(row):
    if row['min_comp'] is None:
        return False, 'min_comp does not exist'


min_comp_check = Working_func(_min_comp_check, 'min comp check')


def _clearance_check(row):
    if row['reg'] is None:
        return None, None
    temp = row['PMI'] if row['PMI'] is not None else row['reg']
    if '{:.2f}'.format(temp)[-2:] in ('87', '88', '93', '97'):
        return False, 'Clearance Block'


clearance_check = Working_func(_clearance_check, 'Clearance Check')


def _pmi_check(row):
    if row['PMI'] is None:
        return False, 'PMI does not exist'


pmi_check = Working_func(_pmi_check, 'PMI Check')


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


def _map_price_check(row):
    if row['MAP_price'] is None:
        return False, 'No MAP Price'


map_price_check = Working_func(_map_price_check, 'MAP Price check')


def _dp_block(row):
    if row['blocked'] is not None:
        return False, 'DP Blocked'


dp_block = Working_func(_dp_block, 'DP Block_Check')


def _div_8_no_VD(row):
    if row['ffm_channel'] is not None and row['ffm_channel'].strip() == 'VD' and row['div_no'] == 8:
        return False, 'div 8 no VD'


div_8_no_VD = Working_func(_div_8_no_VD, 'div 8 VD block')


def _no_TW(row):
    if row['ffm_channel'] is not None and row['ffm_channel'].strip() == 'TW':
        return False, 'no TW'


no_TW = Working_func(_no_TW, 'no TW')

def _no_craftsman(row):
    product_brand = row['Product_Brand'] if row['Product_Brand'] is not None else ''
    brand = row['brand'] if row['brand'] is not None else ''
    if 'craftsman' in product_brand.lower() or 'craftsman' in brand.lower():
        return False, 'No Craftsman'
    if product_brand == '' and brand == '':
        return False, 'No Craftsman'


no_craftsman = Working_func(_no_craftsman, 'No Craftsman')


def _no_kenmore(row):
    product_brand = row['Product_Brand'] if row['Product_Brand'] is not None else ''
    brand = row['brand'] if row['brand'] is not None else ''
    if 'kenmore' in brand.lower():
        return False, 'No Kenmore'
    if 'kenmore' in product_brand.lower() and brand == '':
        return False, 'No Kenmore'


no_kenmore = Working_func(_no_kenmore, 'No Kenmore')


def _kenmore(row):
    product_brand = row['Product_Brand'] if row['Product_Brand'] is not None else ''
    brand = row['brand'] if row['brand'] is not None else ''
    if 'kenmore' not in brand.lower() and 'kenmore' not in product_brand.lower():
        return False, 'not Kenmore'


kenmore = Working_func(_kenmore, 'Kenmore')


def _VD_check(row):
    if row['ffm_channel'] is not None and row['ffm_channel'].strip() != 'VD':
        return False, 'NOT VD ITEM'


VD_check = Working_func(_VD_check, 'VD ITEMS ONLY')


def _ee_check(row):
    if row['ee_price'] is None:
        return False, 'No EE price'


ee_check = Working_func(_ee_check, 'No EE price')


def _PMI_ban(row):
    if row['PMI_exists'] is True:
        return False, 'PMI ban for ee'


PMI_ban = Working_func(_PMI_ban, 'do not send ee for those with PMI ')


def _apparel_brand_check(row):
    product_brand = row['Product_Brand'] if row['Product_Brand'] is not None else ''
    brand = row['brand'] if row['brand'] is not None else ''

    if row['div_no'] in (77, 29) and \
            ('american princess' in product_brand.lower() or 'american princess' in brand.lower()):
        return False, 'apparel pmi brand ban'
    if row['div_no'] in (41, 43) and \
            (('levi' in product_brand.lower() or 'levi' in brand.lower()) or \
             ('docker' in product_brand.lower() or 'docker' in brand.lower())):
        return False, 'apparel pmi brand ban'


apparel_brand_check = Working_func(_apparel_brand_check, 'apparel pmi pricing no touch brand')
