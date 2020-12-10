import statistics
from rqalpha.apis.api_base import get_positions
from rqalpha.mod.rqalpha_mod_sys_accounts.api.api_stock import order_target_value
import logging
logger =logging.getLogger('Trading_small_quality_cap')

## 个股止损
def security_stoploss(context,loss=0.2):
    if len(context.portfolio.positions)>0:
        for stock in context.portfolio.positions.keys():
            avg_cost = context.portfolio.positions[stock].avg_cost
            current_price = context.portfolio.positions[stock].price
            if 1 - current_price/avg_cost >= loss:
                logger.info(str(stock) + '  跌幅达个股止损线，平仓止损！')
                order_target_value(stock, 0)

## 个股止盈
def security_stopprofit(context,profit=0.2):
    if len(context.portfolio.positions)>0:
        for stock in context.portfolio.positions.keys():
            avg_cost = context.portfolio.positions[stock].avg_cost
            current_price = context.portfolio.positions[stock].price
            if current_price/avg_cost - 1 >= profit:
                logger.info(str(stock) + '  涨幅达个股止盈线，平仓止盈！')
                order_target_value(stock, 0)

## 大盘止损
# 根据大盘指数N日均线进行止损
def index_stoploss_sicha(index, context, n=60):
    '''
    当大盘N日均线(默认60日)低于昨日收盘价构成，则清仓止损
    '''
    if len(context.portfolio.positions)>0:
        benchmark_history = #此处取 'benchmark' 所用的指数，要日线行情，取数范围 n+2。 “close" 为指示呢一列
        temp1 = statistics.mean(benchmark_history['close'][1:-1])
        temp2 = statistics.mean(benchmark_history['close'][0:-2])
        close1 = benchmark_history['close'][-1]
        close2 = benchmark_history['close'][-2]
        if (close2 > temp2) and (close1 < temp1):
            logger.info('大盘触及止损线，清仓！')
            for stock in context.portfolio.positions.keys():
                order_target_value(stock, 0)
# 根据大盘指数跌幅进行止损
def index_stoploss_diefu(index, context, n=10, stop_loss_limit=0.03):
    '''
    当大盘N日内跌幅超过止损限额，则清仓止损
    '''
    if len(context.portfolio.positions)>0:
        benchmark_history = #TODO: #此处取 'benchmark' 所用的指数，要日线行情，取数范围 n。 “close" 为指示呢一列
        if ((1-float(benchmark_history['close'][-1]/benchmark_history['close'][0])) >= stop_loss_limit):
            logger.info('大盘触及止损线，清仓！')
            for stock in context.portfolio.positions.keys():
                order_target_value(stock, 0)