# -*- coding: utf-8 -*-

from rqalpha import run_code
code = """
import sys

import pandas as pd
import logging
from datetime import datetime
import talib

#Load Tushare
from rqalpha.apis.api_base import history_bars, get_position
from rqalpha.mod.rqalpha_mod_sys_accounts.api.api_stock import order_target_value, order_value

import Utils.numeric_utils as num_util
import tushare as ts

from sqlalchemy import Column, String, Float, MetaData, Table, create_engine, INT
import Utils.configuration_file_service as config_service

import Utils.logging_util as lu

engine = create_engine(config_service.getDefaultDB())
conn = engine.connect()

file_name = "Trading_small_quality_cap"
logger = logging.getLogger('Trading_small_quality_cap')


def daily_sql(table_name, ts_code: str, start_date: int, end_date: int,
                                              engine) -> pd.DataFrame:
    sql = "select * from " + table_name + " where " + "ts_code=" + '"'+ts_code + '"'+" and trade_date_int between "+ str(start_date) + " and " + str(end_date)
    print(sql)
    return pd.read_sql(sql, engine)


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    context.s1 = "000001.SZ"

    # 设置这个策略当中会用到的参数，在策略中可以随时调用，这个策略使用长短均线，我们在这里设定长线和短线的区间，在调试寻找最佳区间的时候只需要在这里进行数值改动
    context.SHORTPERIOD = 20
    context.LONGPERIOD = 120
    context.table_name = "daily_2015_2019"
    context.ts_code = context.s1
    context.start_date = 20150101
    context.end_date = 20191231
    context.orders = []

# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 开始编写你的主要的算法逻辑
    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合状态信息

    # 使用order_shares(id_or_ins, amount)方法进行落单

    # TODO: 开始编写你的算法吧！

    # 因为策略需要用到均线，所以需要读取历史数据
    print(datetime.now())
    print("entering handlebar for "+ context.now.date().strftime("%Y%m%d"))
    df = daily_sql(context.table_name, context.ts_code, context.start_date, context.end_date, engine)
    print(datetime.now())
    print("done sql "+ context.now.date().strftime("%Y%m%d"))
    prices = df['close'].to_numpy(dtype="float64")

    # 使用talib计算长短两根均线，均线以array的格式表达
    print(datetime.now())
    print("calculating sma and plotting for "+ context.now.date().strftime("%Y%m%d"))
    short_avg = talib.SMA(prices, context.SHORTPERIOD)
    long_avg = talib.SMA(prices, context.LONGPERIOD)

    plot("short avg", short_avg[-1])
    plot("long avg", long_avg[-1])
    # 计算现在portfolio中股票的仓位
    print(datetime.now())
    print("get position for "+ context.now.date().strftime("%Y%m%d"))
    cur_position = get_position(num_util.get_converted_stock_code(context.s1)).quantity

    # 计算现在portfolio中的现金可以购买多少股票
    shares = context.portfolio.cash / bar_dict[num_util.get_converted_stock_code(context.s1)].close

    # 如果短均线从上往下跌破长均线，也就是在目前的bar短线平均值低于长线平均值，而上一个bar的短线平均值高于长线平均值
    if short_avg[-1] - long_avg[-1] < 0 and short_avg[-2] - long_avg[-2] > 0 and cur_position > 0:
        # 进行清仓
        print(datetime.now())
        print("qingcang for "+ context.now.date().strftime("%Y%m%d"))
        order_target_value(num_util.get_converted_stock_code(context.s1), 0)

    # 如果短均线从下往上突破长均线，为入场信号
    if short_avg[-1] - long_avg[-1] > 0 and short_avg[-2] - long_avg[-2] < 0:
        # 满仓入股
        print(datetime.now())
        print("mancang for "+ context.now.date().strftime("%Y%m%d"))
        order_shares(num_util.get_converted_stock_code(context.s1), shares)


def after_trading(context):
    # start = timer()

    #logger.info("Conclusion for date " + context.now.date().strftime("%Y%m%d"))
    #logger.info("Cash:" + str(context.portfolio.cash))
    #logger.info("Net value by yesterday:" + str(context.portfolio.static_unit_net_value))
    #logger.info("Net value by today:" + str(context.portfolio.unit_net_value))
    #logger.info("Profit today:" + str(context.portfolio.daily_pnl))
    #logger.info("Annualized return:" + str(context.portfolio.annualized_returns))
    print(datetime.now())
    print("after trading "+ context.now.date().strftime("%Y%m%d"))
    if (context.now.date().strftime("%Y%m%d") == context.config.base.end_date.strftime("%Y%m%d")):
        pd.DataFrame(context.orders).to_csv('out.csv', index=True)
    # end = timer()
    # print('timestamp5'+str(end - start))


"""

config = {
    'base': {
        'start_date': '2015-01-01',
        'end_date': '2019-12-31',
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


