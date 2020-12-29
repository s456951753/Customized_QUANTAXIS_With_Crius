#找出列表中重复披露的行,取最近的日期
# -*- coding: utf-8 -*-


from datetime import date, datetime, timedelta
import time as t
import time
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

import Utils.numeric_utils as TuRq

#申万一级 成分分类人工选股

today = date.today().strftime("%Y%m%d")
snapshot_date = today

#SW文件读取
stock_list = pd.read_excel (r'C:\Users\Austin\Desktop\Tushare\SW_index_list.xlsx', 'Constituent', index = False)

SW_lv1 = '801790.SI'

"""
Shenwan_code	industry_name	level
801020.SI	采掘	L1
801030.SI	化工	L1
801040.SI	钢铁	L1
801050.SI	有色金属	L1
801710.SI	建筑材料	L1
801720.SI	建筑装饰	L1
801730.SI	电气设备	L1
801890.SI	机械设备	L1
801740.SI	国防军工	L1
801880.SI	汽车	L1
801110.SI	家用电器	L1
801130.SI	纺织服装	L1
801140.SI	轻工制造	L1
801200.SI	商业贸易	L1
801010.SI	农林牧渔	L1
801120.SI	食品饮料	L1
801210.SI	休闲服务	L1
801150.SI	医药生物	L1
801160.SI	公用事业	L1
801170.SI	交通运输	L1
801180.SI	房地产	L1
801080.SI	电子	L1
801750.SI	计算机	L1
801760.SI	传媒	L1
801770.SI	通信	L1
801780.SI	银行	L1
801790.SI	非银金融	L1
801230.SI	综合	L1
"""

#本季度基金公司持股
stock_list = stock_list[stock_list['index_code_lv1']==SW_lv1]
stock_list = stock_list['symbol'].to_list()

print(stock_list)

stock_list_info = []
for stock_code in stock_list:
    all_data = pro.query('daily_basic', ts_code=stock_code, trade_date=snapshot_date, fields='ts_code,turnover_rate_f,volume_ratio,pe_ttm,dv_ratio,free_share,total_mv')
    stock_list_info.append(all_data)
stock_list_info = pd.concat(stock_list_info)

print(stock_list_info)

