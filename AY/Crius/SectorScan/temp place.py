# -*- coding: utf-8 -*-


from datetime import date, datetime, timedelta
import time as t
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

#Load Tushare
from rqalpha.apis.api_base import history_bars, get_position
from rqalpha.mod.rqalpha_mod_sys_accounts.api.api_stock import order_target_value, order_value

import Utils.configuration_file_service as config_service
import tushare as ts

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)

# 主营业务构成
df = pro.balancesheet_vip(ts_code='689009.SH')
print(df)
# Export the df to excel
# df.to_excel(r'C:\Users\Austin\Desktop\Tushare\fina_mainbz_vip.xlsx', index = False)


# data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
# Export the df to excel
# data.to_excel(r'C:\Users\Austin\Desktop\Tushare\stock_basic.xlsx', index = False)
