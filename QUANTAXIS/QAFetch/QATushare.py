# coding: utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2020 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import pandas as pd
import time
from QUANTAXIS.QAUtil import (
    QA_util_date_int2str,
    QA_util_date_stamp,
    QASETTING,
    QA_util_log_info,
    QA_util_to_json_from_pandas
)
import AY.Crius.Utils.configuration_file_service as config_service
import tushare as ts


def set_token(token=None):
    try:
        if token is None:
            # 从~/.quantaxis/setting/config.ini中读取配置
            # token = QASETTING.get_config('TSPRO', 'token', None)
            token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                               property_name=config_service.TS_TOKEN_NAME)
        else:
            QASETTING.set_config('TSPRO', 'token', token)
        ts.set_token(token)
    except:
        if token is None:
            print('请设置tushare的token')
        else:
            print('请升级tushare 至最新版本 pip install tushare -U')


def get_pro():
    try:
        set_token()
        pro = ts.pro_api()
    except Exception as e:
        if isinstance(e, NameError):
            print('请设置tushare pro的token凭证码')
        else:
            print('请升级tushare 至最新版本 pip install tushare -U')
            print(e)
        pro = None
    return pro


def QA_fetch_get_stock_adj(code, end=''):
    """获取股票的复权因子

    Arguments:
        code {[type]} -- [description]

    Keyword Arguments:
        end {str} -- [description] (default: {''})

    Returns:
        [type] -- [description]
    """

    pro = get_pro()
    adj = pro.adj_factor(ts_code=code, trade_date=end)
    return adj


def QA_fetch_stock_basic():
    def fetch_stock_basic():
        stock_basic = None
        try:
            pro = get_pro()
            stock_basic = pro.stock_basic(
                exchange='',
                fields='ts_code,'
                       'symbol,'
                       'name,'
                       'area,industry,list_date'
            )
        except Exception as e:
            print(e)
            print('except when fetch stock basic')
            time.sleep(1)
            stock_basic = fetch_stock_basic()
        return stock_basic

    return fetch_stock_basic()


def cover_time(date):
    """
    字符串 '20180101'  转变成 float 类型时间 类似 time.time() 返回的类型
    :param date: 字符串str -- 格式必须是 20180101 ，长度8
    :return: 类型float
    """
    datestr = str(date)[0:8]
    date = time.mktime(time.strptime(datestr, '%Y%m%d'))
    return date


def _get_subscription_type(if_fq):
    if str(if_fq) in ['qfq', '01']:
        if_fq = 'qfq'
    elif str(if_fq) in ['hfq', '02']:
        if_fq = 'hfq'
    elif str(if_fq) in ['bfq', '00']:
        if_fq = None
    else:
        QA_util_log_info('wrong with fq_factor! using qfq')
        if_fq = 'qfq'
    return if_fq


def QA_fetch_get_stock_day(name, start='', end='', if_fq='qfq', type_='pd'):
    def fetch_data():
        data = None
        try:
            time.sleep(0.002)
            pro = get_pro()
            data = ts.pro_bar(
                api=pro,
                ts_code=str(name),
                asset='E',
                adj=_get_subscription_type(if_fq),
                start_date=start,
                end_date=end,
                freq='D',
                factors=['tor',
                         'vr']
            ).sort_index()
            print('fetch done: ' + str(name))
        except Exception as e:
            print(e)
            print('except when fetch data of ' + str(name))
            time.sleep(1)
            data = fetch_data()
        return data

    data = fetch_data()

    data['date_stamp'] = data['trade_date'].apply(lambda x: cover_time(x))
    data['code'] = data['ts_code'].apply(lambda x: str(x)[0:6])
    data['fqtype'] = if_fq
    if type_ in ['json']:
        data_json = QA_util_to_json_from_pandas(data)
        return data_json
    elif type_ in ['pd', 'pandas', 'p']:
        data['date'] = pd.to_datetime(data['trade_date'], format='%Y%m%d')
        data = data.set_index('date', drop=False)
        data['date'] = data['date'].apply(lambda x: str(x)[0:10])
        return data


def QA_fetch_get_stock_realtime():
    data = ts.get_today_all()
    data_json = QA_util_to_json_from_pandas(data)
    return data_json


def QA_fetch_get_stock_info(name):
    data = ts.get_stock_basics()
    try:
        return data if name == '' else data.loc[name]
    except:
        return None


def QA_fetch_get_stock_tick(name, date):
    if (len(name) != 6):
        name = str(name)[0:6]
    return ts.get_tick_data(name, date)


def QA_fetch_get_stock_list(type='list'):
    df = QA_fetch_stock_basic()
    if (type == 'list'):
        return list(df.ts_code)
    else:
        return df


def QA_fetch_get_stock_time_to_market():
    data = ts.get_stock_basics()
    return data[data['timeToMarket'] != 0]['timeToMarket'] \
        .apply(lambda x: QA_util_date_int2str(x))


def QA_fetch_get_trade_date(end, exchange):
    pro = get_pro()
    data = pro.trade_cal()
    da = data[data.is_open > 0]
    data_json = QA_util_to_json_from_pandas(da)
    message = []
    for i in range(0, len(data_json) - 1, 1):
        date = data_json[i]['cal_date']
        num = i + 1
        exchangeName = 'SSE'
        # data_stamp = QA_util_date_stamp(date)
        mes = {
            'date': date,
            'num': num,
            'exchangeName': exchangeName
            # 'date_stamp': data_stamp
        }
        message.append(mes)
    return message


def QA_fetch_get_lhb(date):
    return ts.top_list(date)


def QA_fetch_get_stock_money():
    pass


def QA_fetch_get_stock_block():
    """Tushare的版块数据
    
    Returns:
        [type] -- [description]
    """
    import tushare as ts
    csindex500 = ts.get_zz500s()
    try:
        csindex500['blockname'] = '中证500'
        csindex500['source'] = 'tushare'
        csindex500['type'] = 'csindex'
        csindex500 = csindex500.drop(['date', 'name', 'weight'], axis=1)
        return csindex500.set_index('code', drop=False)
    except:
        return None


def QA_fetch_get_stock_financial_indicators(ts_code=None, ann_date=None):
    if (ts_code is None and ann_date is None):
        raise ValueError('either ts_code or ann_date has to be provided')

    import AY.Crius.Utils.logging_util as log
    pro = get_pro()
    logger = log.get_logger('QA_fetch_get_financial_indicators')
    try:
        if (not (ts_code is None) and ann_date is None):
            to_insert = pro.fina_indicator_vip(ts_code=ts_code)
            return to_insert
        elif (ts_code is None and not (ann_date is None)):
            to_insert = pro.fina_indicator_vip(ann_date=ann_date)
            return to_insert
        else:
            to_insert = pro.fina_indicator_vip(ts_code=ts_code, ann_date=ann_date)
            return to_insert
    except Exception as e:
        logger.error(e)
        if (ts_code is None):
            reason = "ann_date " + ann_date
        else:
            reason = "ts_code " + ts_code
        logger.error("error processing data for " + reason)


def QA_fetch_get_stock_cashflow(ts_code=None, ann_date=None):
    if (ts_code is None and ann_date is None):
        raise ValueError('either ts_code or ann_date has to be provided')

    import AY.Crius.Utils.logging_util as log
    pro = get_pro()
    logger = log.get_logger('QA_fetch_get_financial_indicators')
    try:
        if (not (ts_code is None) and ann_date is None):
            to_insert = pro.cashflow_vip(ts_code=ts_code)
            return to_insert
        elif (ts_code is None and not (ann_date is None)):
            to_insert = pro.cashflow_vip(ann_date=ann_date)
            return to_insert
        else:
            to_insert = pro.cashflow_vip(ts_code=ts_code, ann_date=ann_date)
            return to_insert
    except Exception as e:
        logger.error(e)
        if (ts_code is None):
            reason = "ann_date " + ann_date
        else:
            reason = "ts_code " + ts_code
        logger.error("error processing data for " + reason)


def QA_fetch_get_stock_daily_basic(trade_date):
    import AY.Crius.Utils.logging_util as log
    pro = get_pro()
    failed = []
    logger = log.get_logger('QA_fetch_get_stock_daily_basic')
    try:
        to_insert = pro.daily_basic(trade_date=trade_date)
        return to_insert
    except Exception as e:
        logger.error(e)
        logger.error("error processing data for date " + trade_date)


def QA_fetch_get_balance_sheet(ts_code=None, ann_date=None):
    """

    :param start_ann_date:
    :param ts_code:
    :param ann_date:
    :return:
    """
    if (ts_code == None and ann_date == None):
        raise ValueError('either ts_code or ann_date has to be provided')

    import AY.Crius.Utils.logging_util as log
    pro = get_pro()
    logger = log.get_logger('QA_fetch_get_balance_sheet')
    try:
        if (not (ts_code is None) and ann_date is None):
            to_insert = pro.balancesheet_vip(ts_code=ts_code)
            return to_insert
        elif (ts_code is None and not (ann_date is None)):
            to_insert = pro.balancesheet_vip(ann_date=ann_date)
            return to_insert
        else:
            to_insert = pro.balancesheet_vip(ts_code=ts_code, ann_date=ann_date)
            return to_insert
    except Exception as e:
        logger.error(e)
        if (ts_code is None):
            reason = "ann_date " + ann_date
        else:
            reason = "ts_code " + ts_code
        logger.error("error processing data for " + reason)


# test

# print(get_stock_day("000001",'2001-01-01','2010-01-01'))
# print(get_stock_tick("000001.SZ","2017-02-21"))
if __name__ == '__main__':
    df = QA_fetch_get_stock_list()
    print(df)
