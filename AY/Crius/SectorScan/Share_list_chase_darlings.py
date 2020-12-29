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

#df2 = pro.index_classify(level='L2', src='SW')

#日期格式为 月/日/年
start_date = '4/1/2020'
end_date = '7/1/2020'
start_date_p = '1/1/2020'
end_date_p = '4/1/2020'

#本季度日期范围
datelist = pd.date_range(start=start_date, end=end_date)
date_strings = [datetime.strftime(d, '%Y%m%d') for d in datelist]

#上季度日期范围
datelist_prior = pd.date_range(start=start_date_p, end=end_date_p)
date_strings_prior = [datetime.strftime(d, '%Y%m%d') for d in datelist_prior]

#所有季度基金公司持股文件读取
fund_list = pd.read_excel (r'C:\Users\Austin\Desktop\Tushare\Tushare_Fund_data.xlsx', index = False)

#本季度基金公司持股
fund_list_snapshot = fund_list[fund_list.end_date.isin(date_strings)]

#新建一个column使得每个row的数据都是唯一的。第一步把end_date 改成string 格式
fund_list_snapshot2 = fund_list_snapshot.copy()
fund_list_snapshot2.loc[:, 'end_date2'] = fund_list_snapshot2['end_date'].apply(str)
fund_list_snapshot2.loc[:, 'uniquecode1'] = fund_list_snapshot2.ts_code + "_" + fund_list_snapshot2.end_date2
fund_list_snapshot2.loc[:, 'uniquecode2'] = fund_list_snapshot2.ts_code + "_" + fund_list_snapshot2.symbol + "_" + fund_list_snapshot2.end_date2

#找出列表中重复披露的行,取最近的日期
keep_list = fund_list_snapshot.copy()
keep_list.loc[:, 'uniquecode1'] = keep_list.ts_code + "_" + keep_list['end_date'].apply(str)
keep_list1 = keep_list.drop_duplicates(subset=['ts_code'], keep='first')
keep_list1 = keep_list1['uniquecode1'].to_list()

#选取列表中唯一披露的行
fund_list_snapshot2 = fund_list_snapshot2[fund_list_snapshot2['uniquecode1'].isin(keep_list1)]

#最终列表整理
fund_list_snapshot2 = pd.pivot_table(fund_list_snapshot2,index=["symbol"],aggfunc={'stk_float_ratio':np.sum,'ts_code':np.count_nonzero,'mkv':np.sum,'amount':np.sum})
fund_list_snapshot2 = fund_list_snapshot2.sort_values(by='stk_float_ratio', ascending=False)

#上季度基金公司持股
fund_list_snapshot_prior = fund_list[fund_list.end_date.isin(date_strings_prior)]

#新建一个column使得每个row的数据都是唯一的。第一步把end_date 改成string 格式
fund_list_snapshot_prior2 = fund_list_snapshot_prior.copy()
fund_list_snapshot_prior2.loc[:, 'end_date2'] = fund_list_snapshot_prior2['end_date'].apply(str)
fund_list_snapshot_prior2.loc[:, 'uniquecode1'] = fund_list_snapshot_prior2.ts_code + "_" + fund_list_snapshot_prior2.end_date2
fund_list_snapshot_prior2.loc[:, 'uniquecode2'] = fund_list_snapshot_prior2.ts_code + "_" + fund_list_snapshot_prior2.symbol + "_" + fund_list_snapshot_prior2.end_date2

#找出列表中重复披露的行,取最近的日期
keep_list_prior = fund_list_snapshot_prior.copy()
keep_list_prior.loc[:, 'uniquecode1'] = keep_list_prior.ts_code + "_" + keep_list_prior['end_date'].apply(str)
keep_list_prior1 = keep_list_prior.drop_duplicates(subset=['ts_code'], keep='first')
keep_list_prior1 = keep_list_prior1['uniquecode1'].to_list()

#选取列表中唯一披露的行
fund_list_snapshot_prior2 = fund_list_snapshot_prior2[fund_list_snapshot_prior2['uniquecode1'].isin(keep_list_prior1)]

#最终列表整理
fund_list_snapshot_prior2 = pd.pivot_table(fund_list_snapshot_prior2,index=["symbol"],aggfunc={'stk_float_ratio':np.sum,'ts_code':np.count_nonzero,'mkv':np.sum,'amount':np.sum})
fund_list_snapshot_prior2 = fund_list_snapshot_prior2.sort_values(by='stk_float_ratio', ascending=False)

#Export the df to excel
fund_list_snapshot2.to_excel(r'C:\Users\Austin\Desktop\Tushare\fund_list_snapshot.xlsx', index = True)
fund_list_snapshot_prior2.to_excel(r'C:\Users\Austin\Desktop\Tushare\fund_list_snapshot_prior.xlsx', index = True)

#本季度和上一季度基金公司持股文件读取
fund_list = pd.read_excel (r'C:\Users\Austin\Desktop\Tushare\fund_list_snapshot.xlsx', index = False)
fund_list_prior = pd.read_excel (r'C:\Users\Austin\Desktop\Tushare\fund_list_snapshot_prior.xlsx', index = False)

#上季度列名称改名，防止重复
fund_list_prior = fund_list_prior.rename(columns={'amount': 'amount_p', 'mkv': 'mkv_p', 'stk_float_ratio': 'stk_float_ratio_p', 'ts_code': 'ts_code_p'})

#合并两个基金表格
#summary_fund_list = pd.concat([fund_list, fund_list_prior], axis=1, join='outer', sort=False)

summary_fund_list = pd.merge(fund_list, fund_list_prior, on='symbol', how='outer')
summary_fund_list = summary_fund_list.fillna(0)

#计算两个基金表格，寻找差值
summary_fund_list['stk_float_ratio_diff'] = summary_fund_list['stk_float_ratio'] - summary_fund_list['stk_float_ratio_p']
summary_fund_list = summary_fund_list.sort_values(by='stk_float_ratio_diff', ascending=False)

#剔除港股，增加申万分类
summary_fund_list['exchange'] = summary_fund_list['symbol'].str[-2:]
summary_fund_list = summary_fund_list[~summary_fund_list.symbol.str.contains('HK')]

SW_index_list = fund_list = pd.read_excel (r'C:\Users\Austin\Desktop\Tushare\SW_index_list.xlsx', sheet_name='Constituent', index = False)
SW_index_list = SW_index_list.loc[:, ['symbol','industry_name_lv1']]

summary_fund_list2 = pd.merge(summary_fund_list, SW_index_list, on = 'symbol', how = 'left')

#Export the df to excel
summary_fund_list2.to_excel(r'C:\Users\Austin\Desktop\Tushare\summary_fund_list.xlsx', index = True)


