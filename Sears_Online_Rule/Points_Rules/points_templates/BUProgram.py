import re
import string
from functools import partial
from harlem125.dp_rules import Working_func
from operator import itemgetter
bu_lst = [
    '''BU - Apparel''',
    '''BU - ACCESSORIES''',
    '''BU - Appliance & Hardware''',
    '''BU - Appliance''',
    '''BU - AUTOMOTIVE''',
    '''BU - Auto''',
    '''BU-Basket''',
    '''BU - CHLDRNS APPAREL/HARDLNS''',
    '''BU - CCN (Customer Care Network)''',
    '''BU - CONSUMER ELECTRONICS''',
    '''BU - CIRCLE OF BEAUTY''',
    '''BU - COSMETICS/FRAGRANCES''',
    '''BU - Craftsman''',
    '''BU - Drug''',
    '''BU - DEALER STORES''',
    '''BU - Electronics''',
    '''BU - Footwear''',
    '''BU - FINE JEWELRY''',
    '''BU - FURNITURE''',
    '''BU - Grocery''',
    '''BU - HOME APPLIANCES''',
    '''BU - Home Appliance Showroom''',
    '''BU - HOME FASHIONS''',
    '''BU - HOUSEHOLD GOODS''',
    '''BU - HOME DECOR''',
    '''BU - Home''',
    '''BU - HARDLINES & SOFTLINES & COMMOD''',
    '''BU - Hometown''',
    '''BU - INTIMATE APPAREL''',
    '''BU - Jewelry''',
    '''BU - Lands' End''',
    '''BU - LANDS END''',
    '''BU - Lawn & Garden''',
    '''BU - LAWN/HOME IMP/FITNESS''',
    '''BU - LICENSED BUSINESS''',
    '''BU - MEN'S APPAREL''',
    '''BU - MELDISCO''',
    '''BU - MISCELLANEOUS BUSINESS''',
    '''BU - MISCELLANEOUS''',
    '''BU - Multi-BU''',
    '''BU - NON-MERCHANDISE''',
    '''BU - Outlet''',
    '''BU-OBU''',
    '''BU - Partner''',
    '''BU - Pharmacy''',
    '''BU - READY TO WEAR''',
    '''BU - Sweepstakes''',
    '''BU - Sporting Goods''',
    '''BU - SHO''',
    '''BU - Seasonal/Outdoor Living''',
    '''BU - SUPERK''',
    '''BU - TRAFFIC AND TRANSACTION''',
    '''BU - Toys''',
    '''BU - TOOLS/PAINT'''
]



def _buprogram_by_div_ln(row, bu_expense):
    if row['div_no'] in (20, 22, 26, 42, 46):
        return 'BU - HOME APPLIANCES', 'BU HA'
    elif row['div_no'] == 6:
        return 'BU - Sporting Goods', 'BU SPG'
    elif row['div_no'] == (9, 30, 34):
        return 'BU - TOOLS/PAINT', 'BU TOOLS'
    elif row['div_no'] in (8, 14, 24, 96):
        return 'BU - Home', 'BU - Home'
    elif row['div_no'] in (29, 49, 77):
        return 'BU - CHLDRNS APPAREL/HARDLNS', 'BU - CHLDRNS APPAREL/HARDLNS'
    elif row['div_no'] == 52:
        return 'BU - Toys', 'BU - Toys'
    elif row['div_no'] == 71 and row['ln_no'] not in (22, 28, 29, 63, 66, 67):
        return 'BU - Lawn & Garden', 'BU - Lawn & Garden'
    elif row['div_no'] == 71 and row['ln_no'] in (22, 28, 29, 63, 66, 67):
        return 'BU - Seasonal/Outdoor Living', 'BU - Seasonal/Outdoor Living'
    elif row['div_no'] == 95:
        return 'BU - Auto', 'BU - Auto'
    matchlst = []
    for idx, eachBU in enumerate(bu_expense):
        if row['VBS_Name'] is None:
            return None, None
        vbs_name = ''.join([x for x in row['VBS_Name'] if x in string.ascii_letters]).lower()
        bu_exp = ''.join([x for x in eachBU if x in string.ascii_letters]).lower()
        if vbs_name in bu_exp:
            matchlst.append((idx, len(bu_exp)))
    if len(matchlst)>0:
        matchlst_sorted = sorted(matchlst, key=itemgetter(1), reverse=True)
        return bu_expense[matchlst_sorted[0][0]], 'Matched bu by BU finder'
    return None, None

buprogram_by_div_ln = Working_func(partial(_buprogram_by_div_ln, bu_expense = bu_lst), 'BU PROGRAM FINDER')


def _buprogram_Multi_BU(row):
    return 'BU - Multi-BU', 'BU - Multi-BU'

buprogram_Multi_BU = Working_func(_buprogram_Multi_BU, 'BU - Multi-BU Rule')
