# -*- coding: utf-8 -*-


from datetime import date, datetime, timedelta
import time as t
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import talib

#Load Tushare
from rqalpha.apis.api_base import history_bars, get_position
from rqalpha.mod.rqalpha_mod_sys_accounts.api.api_stock import order_target_value, order_value

import Utils.configuration_file_service as config_service
import tushare as ts

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)


#主营业务构成
df = pro.fina_mainbz_vip(period='20191231', type='P' ,fields='ts_code,end_date,bz_item,bz_sales')

#Export the df to excel
df.to_excel(r'C:\Users\Austin\Desktop\Tushare\fina_mainbz_vip.xlsx', index = False)


data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
#Export the df to excel
data.to_excel(r'C:\Users\Austin\Desktop\Tushare\stock_basic.xlsx', index = False)