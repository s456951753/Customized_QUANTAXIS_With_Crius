import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import itertools

#Load Tushare
from rqalpha.apis.api_base import history_bars, get_position
from rqalpha.mod.rqalpha_mod_sys_accounts.api.api_stock import order_target_value, order_value

import Utils.configuration_file_service as config_service
import tushare as ts

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)

df = pro.query('daily', ts_code='000001.SZ', start_date='20180701', end_date='20190718')


#---------------------------------------------------------------------------

def ma(df, n=10):
    """
    移动平均线 Moving Average
    MA（N）=（第1日收盘价+第2日收盘价—+……+第N日收盘价）/N
    """
    pv = pd.DataFrame()
    pv['date'] = df['trade_date']
    pv['v'] = df.close.rolling(n).mean()
    return pv

def _ma(series, n):
    """
    移动平均
    """
    return series.rolling(n).mean()

def md(df, n=10):
    """
    移动标准差
    STD=S（CLOSE,N）=[∑（CLOSE-MA(CLOSE，N)）^2/N]^0.5
    """
    _md = pd.DataFrame()
    _md['date'] = df['trade_date']
    _md['md'] = df.close.rolling(n).std(ddof=0)
    return _md

def _md(series, n):
    """
    标准差MD
    """
    return series.rolling(n).std(ddof=0)  # 有时候会用ddof=1

def ema(df, n=12):
    """
    指数平均数指标 Exponential Moving Average
    今日EMA（N）=2/（N+1）×今日收盘价+(N-1)/（N+1）×昨日EMA（N）
    EMA(X,N)=[2×X+(N-1)×EMA(ref(X),N]/(N+1)
    """
    _ema = pd.DataFrame()
    _ema['date'] = df['trade_date']
    _ema['ema'] = df.close.ewm(ignore_na=False, span=n, min_periods=0, adjust=False).mean()
    return _ema

def _ema(series, n):
    """
    指数平均数
    """
    return series.ewm(ignore_na=False, span=n, min_periods=0, adjust=False).mean()

def macd(df, n=12, m=26, k=9):
    """
    平滑异同移动平均线(Moving Average Convergence Divergence)
    今日EMA（N）=2/（N+1）×今日收盘价+(N-1)/（N+1）×昨日EMA（N）
    DIFF= EMA（N1）- EMA（N2）
    DEA(DIF,M)= 2/(M+1)×DIF +[1-2/(M+1)]×DEA(REF(DIF,1),M)
    MACD（BAR）=2×（DIF-DEA）
    return:
          osc: MACD bar / OSC 差值柱形图 DIFF - DEM
          diff: 差离值
          dea: 讯号线
    """
    _macd = pd.DataFrame()
    _macd['date'] = df['trade_date']
    _macd['diff'] = _ema(df.close, n) - _ema(df.close, m)
    _macd['dea'] = _ema(_macd['diff'], k)
    _macd['macd'] = _macd['diff'] - _macd['dea']
    return _macd

def sma(a, n, m=1):
    """
    平滑移动指标 Smooth Moving Average
    """
    results = np.nan_to_num(a).copy()
    # FIXME this is very slow
    for i in range(1, len(a)):
        results[i] = (m * results[i] + (n - m) * results[i - 1]) / n
        results[i] = ((n - 1) * results[i - 1] + results[i]) / n
    return results

    b = np.nan_to_num(a).copy()
    return ((n - m) * a.shift(1) + m * a) / n

    a = a.fillna(0)
    b = a.ewm(min_periods=0, ignore_na=False, adjust=False, alpha=m/n).mean()
    return b

def kdj(df, n=9):
    """
    随机指标KDJ
    N日RSV=（第N日收盘价-N日内最低价）/（N日内最高价-N日内最低价）×100%
    当日K值=2/3前1日K值+1/3×当日RSV=SMA（RSV,M1）
    当日D值=2/3前1日D值+1/3×当日K= SMA（K,M2）
    当日J值=3 ×当日K值-2×当日D值
    """
    _kdj = pd.DataFrame()
    _kdj['date'] = df['trade_date']
    rsv = (df.close - df.low.rolling(n).min()) / (df.high.rolling(n).max() - df.low.rolling(n).min()) * 100
    _kdj['k'] = sma(rsv, 3)
    _kdj['d'] = sma(_kdj.k, 3)
    _kdj['j'] = 3 * _kdj.k - 2 * _kdj.d
    return _kdj

def rsi(df, n=6):
    """
    相对强弱指标（Relative Strength Index，简称RSI
    LC= REF(CLOSE,1)
    RSI=SMA(MAX(CLOSE-LC,0),N,1)/SMA(ABS(CLOSE-LC),N1,1)×100
    SMA（C,N,M）=M/N×今日收盘价+(N-M)/N×昨日SMA（N）
    """
    # pd.set_option('display.max_rows', 1000)
    _rsi = pd.DataFrame()
    _rsi['date'] = df['trade_date']
    px = df.close - df.close.shift(1)
    px[px < 0] = 0
    _rsi['rsi'] = sma(px, n) / sma((df['close'] - df['close'].shift(1)).abs(), n) * 100
    def tmax(x):
        if x < 0:
            x = 0
        return x
    _rsi['rsi'] = sma((df['close'] - df['close'].shift(1)).apply(tmax), n) / sma((df['close'] - df['close'].shift(1)).abs(), n) * 100
    return _rsi

def vrsi(df, n=6):
    """
    量相对强弱指标
    VRSI=SMA（最大值（成交量-REF（成交量，1），0），N,1）/SMA（ABS（（成交量-REF（成交量，1），N，1）×100%
    """
    _vrsi = pd.DataFrame()
    _vrsi['date'] = df['trade_date']
    px = df['vol'] - df['vol'].shift(1)
    px[px < 0] = 0
    _vrsi['vrsi'] = sma(px, n) / sma((df['vol'] - df['vol'].shift(1)).abs(), n) * 100
    return _vrsi

def atr(df, n=14):
    """
    真实波幅	average true range atr(14)
    TR:MAX(MAX((HIGH-LOW),ABS(REF(CLOSE,1)-HIGH)),ABS(REF(CLOSE,1)-LOW))
    ATR:MA(TR,N)
    """
    _atr = pd.DataFrame()
    _atr['date'] = df['trade_date']
#    _atr['tr'] = np.maximum(df.high - df.low, (df.close.shift(1) - df.low).abs())
#    _atr['tr'] = np.maximum.reduce([df.high - df.low, (df.close.shift(1) - df.high).abs(), (df.close.shift(1) - df.low).abs()])
    _atr['tr'] = np.vstack([df.high - df.low, (df.close.shift(1) - df.high).abs(), (df.close.shift(1) - df.low).abs()]).max(axis=0)
    _atr['atr'] = _atr.tr.rolling(n).mean()
    return _atr

def up_n(df):
    """
    连涨天数	up_n	连续上涨天数，当天收盘价大于开盘价即为上涨一天 # 同花顺实际结果用收盘价-前一天收盘价
    """
    _up = pd.DataFrame()
    _up['date'] = df['trade_date']
    p = df.close - df.close.shift()
    p[p > 0] = 1
    p[p < 0] = 0
    m = []
    for k, g in itertools.groupby(p):
        t = 0
        for i in g:
            if k == 0:
                m.append(0)
            else:
                t += 1
                m.append(t)
    # _up['p'] = p
    _up['up'] = m
    return _up

def down_n(df):
    """
    连跌天数	down_n	连续下跌天数，当天收盘价小于开盘价即为下跌一天
    """
    _down = pd.DataFrame()
    _down['date'] = df['trade_date']
    p = df.close - df.close.shift()
    p[p > 0] = 0
    p[p < 0] = 1
    m = []
    for k, g in itertools.groupby(p):
        t = 0
        for i in g:
            if k == 0:
                m.append(0)
            else:
                t += 1
                m.append(t)
    _down['down'] = m
    return _down