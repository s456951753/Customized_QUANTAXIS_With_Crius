# -*- coding: utf-8 -*-

from rqalpha import run_code

code = """

from rqalpha.api import *

import Utils.numeric_utils as TuRq
from datetime import datetime, timedelta
from datetime import date
import time as t
import pandas as pd
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

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):

    # 选择我们感兴趣的股票
    context.small_cap_cutoff_up = 1150000 #defind the cutoff for small caps upper end （unit=万元）Note the cutoff is consistent with MSCI small cap definition
    context.small_cap_cutoff_low = 1000000 #defind the cutoff for small caps lower end （unit=万元）
    context.pe_cutoff_up = 5 #defind the cutoff for stock PE ratio
    
    context.TIME_PERIOD = 14
    context.HIGH_RSI = 85
    context.LOW_RSI = 30
    context.ORDER_PERCENT = 0.1


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 开始编写你的主要的算法逻辑

    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合状态信息

    # 使用order_shares(id_or_ins, amount)方法进行落单

    # TODO: 开始编写你的算法吧！

    # 对我们选中的股票集合进行loop，运算每一只股票的RSI数值
    d0 = context.now.date()
    snapshot_date = d0.strftime("%Y%m%d")
    
    sector_full_list_snapshot = pro.query('daily_basic', ts_code='', trade_date=snapshot_date,fields='ts_code,turnover_rate_f,volume_ratio,pe_ttm,dv_ratio,free_share,total_mv')
    
    list1 = sector_full_list_snapshot[sector_full_list_snapshot['total_mv'].between(context.small_cap_cutoff_low, context.small_cap_cutoff_up) & sector_full_list_snapshot['pe_ttm'].between(0.01, context.pe_cutoff_up)]
    
    list1 = list1['ts_code'].to_list()
    
    context.stocks = TuRq.get_list_of_converted_stock_code(list1)
    
    for stock in context.stocks:
        # 读取历史数据
        prices = history_bars(stock, context.TIME_PERIOD+1, '1d', 'close')

        # 用Talib计算RSI值
        rsi_data = talib.RSI(prices, timeperiod=context.TIME_PERIOD)[-1]

        cur_position = get_position(stock).quantity
        # 用剩余现金的30%来购买新的股票
        target_available_cash = context.portfolio.cash * context.ORDER_PERCENT

        # 当RSI大于设置的上限阀值，清仓该股票
        if rsi_data > context.HIGH_RSI and cur_position > 0:
            order_target_value(stock, 0)

        # 当RSI小于设置的下限阀值，用剩余cash的一定比例补仓该股
        if rsi_data < context.LOW_RSI:
            logger.info("target available cash caled: " + str(target_available_cash))
            # 如果剩余的现金不够一手 - 100shares，那么会被ricequant 的order management system reject掉
            order_value(stock, target_available_cash)
"""

config = {
  "base": {
    "start_date": "2019-01-01",
    "end_date": "2019-09-01",
    "benchmark": "000300.XSHG",
    "accounts": {
      "stock": 1000000
    }
  },
  "extra": {
    "log_level": "verbose",
  },
  "mod": {
    "sys_analyser": {
      "enabled": True,
      "plot": True
    }
  }
}

run_code(code, config)
