# -*- coding: utf-8 -*-

from rqalpha import run_code

code = """
from rqalpha.api import *


import numpy as np
import math
import Utils.numeric_utils as TuRq
from datetime import date, datetime, timedelta
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

#选取符合条件的股票
def query_fundamental(context, bar_dict):
    d0 = context.now.date()
    d1 = d0.strftime("%Y%m%d")
    list = pro.daily(trade_date=d1)
    list = list.sort_values(by='pct_chg', ascending=False).head(5)
    list = list['ts_code'].to_list()
    
    # 将查询结果dataframe存放在context里面以备后面只需：
    context.stocks= TuRq.get_list_of_converted_stock_code(list)

    # 实时打印日志看下查询结果：
    logger.info(context.stocks)

    # 对于每一个股票按照平均现金买入：
    update_universe(context.stocks)

    stocksNumber = len(context.stocks)
    context.average_percent = 0.99 / stocksNumber
    logger.info("Calculated average percent for each stock is: %f" % context.average_percent)

    # 先查一下选出来的股票是否在已有的portfolio里面：
    # 这样做并不是最好的，只是代码比较简单
    # 先清仓然后再买入这一个月新的符合条件的股票
    logger.info("Clearing all the current positions.")
    for holding_stock in context.portfolio.positions.keys():
        if context.portfolio.positions[holding_stock].quantity != 0:
            order_target_percent(holding_stock, 0)

    logger.info("Building new positions for portfolio.")
    for stock in context.stocks:
        order_target_percent(stock, context.average_percent)
        logger.info("Buying: " + str(context.average_percent) + " % for stock: " + str(stock))

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    scheduler.run_monthly(query_fundamental, monthday=1)


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    pass
    
"""


config = {
  'base': {
    'start_date': '2016-06-01',
    'end_date': '2017-12-01',
    # 回测频率，1d, 1m, tick
    'frequency': '1d',
    # 回测所需 bundle 数据地址，可设置为 RQPro 终端【个人设置】的【数据下载路径】
    #    'data_bundle_path': './bundle',
    # 策略文件地址
    #    'strategy_file': './strategy.py',
    # 保证金倍率。基于基础保证金水平进行调整
    'margin_multiplier': 1,
    # 运行类型。b 为回测，p 为模拟交易，r 为实盘交易
    'run_type': 'b',
    # 基准合约
    'benchmark': '000300.XSHG',

    # 账户类别及初始资金
    'accounts': {
      'stock': 100000
    },
  },
  'extra': {
    # 是否开启性能分析
    'enable_profiler': False,
    # 输出日志等级，有 verbose, info, warning, error 等选项，可以通过设置为 verbose 来查看最详细日志
    'log_level': 'verbose',
  },
  'mod': {
    # 模拟撮合模块
    'sys_simulation': {
        'enabled': True,
        # 是否开启信号模式。如果开启，限价单将按照指定价格成交，并且不受撮合成交量限制
        'signal': False,
        # 撮合方式。current_bar 当前 bar 收盘价成交，next_bar 下一 bar 开盘价成交，best_own 己方最优价格成交（tick 回测使用）
        # best_counterparty 对手方最优价格成交（tick 回测使用），last 最新价成交（tick 回测使用）
        'matching_type': 'current_bar',
        # 是否允许涨跌停状态下买入、卖出
        'price_limit': False,
        # 是否开启成交量限制
        'volume_limit': True,
        # 按照 bar 数据成交量的一定比例进行限制，超限部分无法在当前 bar 一次性撮合成交
        'volume_percent': 0.25,
        # 滑点模型。PriceRatioSlippage 为基于价格比例的滑点模型，TickSizeSlippage 为基于最小价格变动单位的滑点模型
        'slippage_model': 'PriceRatioSlippage',
        # 滑点值
        'slippage': 0,
    },
    # 风控模块
    'sys_risk': {
        'enabled': True,
        # 检查可用资金是否足够
        'validate_cash': True,
        # 检查可平仓位是否足够
        'validate_position': True,
    },
    # 分析模块
    'sys_analyser': {
        'enabled': True,
        # 是否画图
        'plot': True,
        # 指定输出回测报告 csv 路径
        'report_save_path': None,
    },
    'sys_transaction_cost': {
        'enabled': True,
        # 设置最小佣金费用
        'cn_stock_min_commission': 5,
        # 佣金倍率
        'commission_multiplier': 1,
    }
  }
}

run_code(code, config)
