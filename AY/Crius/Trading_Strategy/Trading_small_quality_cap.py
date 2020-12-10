# -*- coding: utf-8 -*-
from rqalpha import run_code

#code = """
from rqalpha.api import *
from timeit import default_timer as timer

import sys

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
import Utils.numeric_utils as num_util
import tushare as ts

import logging
import Utils.logging_util as lu
import builtins
logger =logging.getLogger('Trading_small_quality_cap')
token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)
ts.set_token(token)


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):

    # 选择我们感兴趣的股票
    context.small_cap_cutoff_up = 1150000 #defind the cutoff for small caps upper end （unit=万元）Note the cutoff is consistent with MSCI small cap definition
    context.small_cap_cutoff_low = 500000 #defind the cutoff for small caps lower end （unit=万元）
    context.pe_cutoff_up = 30 #defind the cutoff for stock PE ratio
    
    #剔除过去x天内有涨停的的股票
    context.daysx = 6
    
    context.TIME_PERIOD = 14
    context.HIGH_RSI = 80
    context.LOW_RSI = 20
    context.ORDER_PERCENT = 0.2

    context.days_on_market =730
    context.orders=[]
    logger.setLevel("DEBUG")
    logger.addHandler(logging.StreamHandler(stream=sys.stdout))
    
def handle_bar(context, bar_dict):
    
    # 对我们选中的股票集合进行loop，运算每一只股票的RSI数值
    current_date = context.now.date()
    snapshot_date = current_date.strftime("%Y%m%d")
    print("querying calendar")
    trade_calendar = pro.query('trade_cal', start_date=(current_date- timedelta(days=30)).strftime("%Y%m%d"), end_date=snapshot_date)
    #if today is not a trading day, skip
    if trade_calendar['is_open'].tolist()[-1]==0:
        logger.debug("skipping "+snapshot_date +" because it's not a trading day")
        pass
    else:
        logger.debug("begin trading on "+ snapshot_date)    
    print("starting trading on " + snapshot_date)
    sector_full_list_snapshot = pro.query('daily_basic', ts_code='', trade_date=snapshot_date,
                                          fields='ts_code,turnover_rate_f,volume_ratio,pe_ttm,dv_ratio,free_share,total_mv')
    
    #以股票的 市值(mv) 和 市盈率(PE) 进行筛选
    list1 = sector_full_list_snapshot[sector_full_list_snapshot['total_mv'].between(context.small_cap_cutoff_low, context.small_cap_cutoff_up) & sector_full_list_snapshot['pe_ttm'].between(0.01, context.pe_cutoff_up)]
    
    #剔除过去x天内有涨停的的股票 (x 在init 定义）
    
    #up_start_date = current_date - timedelta(days=context.daysx) #TODO: change to trading days. (https://tushare.pro/document/2?doc_id=26)
    #up_start_date = up_start_date.strftime("%Y%m%d")
    #up_end_date = current_date - timedelta(days=1)
    #up_end_date = up_end_date.strftime("%Y%m%d")
    # get trading day
    up_start_date = num_util.get_last_x_trading_day(list_of_trading_date=trade_calendar,x_day_ago=6)
    up_end_date = num_util.get_last_x_trading_day(list_of_trading_date=trade_calendar,x_day_ago=2)
    up_list = pro.limit_list(start_date=up_start_date, end_date=up_end_date,limit_type='U')
    up_list = up_list['ts_code'].to_list()
    
    

    list2 = list1[~list1.ts_code.isin(up_list)]

    #filter on stocks with at least three years trading history #TODO: the three year variable should be an input field 730 改为人工输入变量，以年为单位
    list_days_filter = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,list_date')
    list_days_filter['list_date'] = pd.to_datetime(list_days_filter['list_date'],format='%Y%m%d')
    list_days_filter['snapshot_date'] = snapshot_date
    list_days_filter['snapshot_date'] = pd.to_datetime(list_days_filter['snapshot_date'],format='%Y%m%d')
    list_days_filter['list_days'] = (list_days_filter['snapshot_date'] - list_days_filter['list_date']).dt.days
    list_days_filter2 = list_days_filter[list_days_filter['list_days'] > context.days_on_market]
    list_days_filter2 = list_days_filter2['ts_code'].to_list()

    list3 = list2[list2.ts_code.isin(list_days_filter2)]
    
    list3 = list3['ts_code'].to_list()
    
    context.stocks = TuRq.get_list_of_converted_stock_code(list3)
    
    for stock in context.stocks:
        #start = timer()

        logger.debug('processing stock:'+stock)
        # The start date of tushare data retrieving
        start_date=(context.now-timedelta(context.TIME_PERIOD + 5)).strftime('%Y%m%d')   #TODO: link timedelta to context.TIME_PERIOD + 5
        
        # 读取历史数据
        # prices = history_bars(stock, context.TIME_PERIOD+1, '1d', 'close')
        # replace rqalpha data with tushare data
        tusharestock = TuRq.get_converted_stock_code(stock)
        #end = timer()
        #print('timestamp1 '+str(end - start))
        #start = timer()
        # TODO - if there is an API to bulky load stock qfq data
        prices = ts.pro_bar(ts_code=tusharestock, adj='qfq', start_date=start_date,end_date=snapshot_date)
        #end = timer()
        #print('timestamp2 '+str(end - start))
        
        #start = timer()
        # 用Talib计算RSI值
        if prices.empty:
            continue    
        rsi_data = talib.RSI(prices['close'], timeperiod=context.TIME_PERIOD).tolist()[-1]
        #end = timer()
        #print('timestamp3 '+str(end - start))
        
        #start = timer()

        cur_position = get_position(stock).quantity
        target_available_cash = context.portfolio.cash * context.ORDER_PERCENT

        if rsi_data > context.HIGH_RSI and cur_position > 0:
            logger.debug("RSI "+str(rsi_data)+" 大于设置的上限阀值" + context.HIGH_RSI + ",清仓 "+ stock)  # TODO: logo 可视化，来查看股票买卖后账户资金的变化
            order=order_target_value(stock, 0)
            if(order == None):
                logger.debug("order is not fulfilled. skip today")
                continue
            lu.get_info_for_order(order,context.portfolio,logger,logging_level="debug")
            context.orders.append(order)

        if rsi_data < context.LOW_RSI:
            logger.debug("RSI "+str(rsi_data)+" 小于设置的下限阀值，用剩余cash的一定比例补仓"+stock)
            logger.info("target available cash caled: " + str(target_available_cash))
            order=order_value(stock, target_available_cash)
            if(order == None):
                logger.debug("order is not fulfilled. skip today")
                continue
            lu.get_info_for_order(order,context.portfolio,logger,logging_level="debug")
            context.orders.append(order)
        #end = timer()
        #print('timestamp4 '+str(end - start))
def after_trading(context):
    #start = timer()
    logger.info("Conclusion for date " + context.now.date().strftime("%Y%m%d"))
    logger.info("Cash:" + str(context.portfolio.cash))
    logger.info("Net value by yesterday:" + str(context.portfolio.static_unit_net_value))
    logger.info("Net value by today:" + str(context.portfolio.unit_net_value))
    logger.info("Profit today:" + str(context.portfolio.daily_pnl))
    logger.info("Annualized return:" + str(context.portfolio.annualized_returns))
    if(context.now.date().strftime("%Y%m%d") == context.config.base.end_date.strftime("%Y%m%d")):
        pd.DataFrame(context.orders).to_csv('out.csv',index=True)
    #end = timer()
    #print('timestamp5'+str(end - start))
        




"""

config = {
    'base': {
        'start_date': '2017-07-07',
        'end_date': '2017-07-09',
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
            'stock': 1000000
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
