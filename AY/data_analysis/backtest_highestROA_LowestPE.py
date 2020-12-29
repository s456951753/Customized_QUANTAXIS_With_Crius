from collections import OrderedDict
from operator import itemgetter

import QUANTAXIS.QAFetch.QAQuery as dbQuery
from QUANTAXIS.QAUtil import DATABASE
import pandas as pd
from pandas.io.json import json_normalize

import AY.Crius.Utils.configuration_file_service as config_service
import AY.Crius.Utils.numeric_utils as nu
import AY.Crius.Utils.trading_calendar_utils as calendar_util


def __getSortedROA__(mongoDB=DATABASE.finacial_indicator):
    '''
    get roa of stocks, sorted by roa from high to low. Using data: tushare's financial indicator

    :param mongoDB:
    :return:
    '''
    '''
    pipeline = [
        {
            '$project': {
                'ts_code': 1,
                'ann_date': 1,
                'roa': {
                    '$ifNull': [
                        '$roa', -999
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$ts_code',
                'date': {
                    '$max': '$ann_date'
                },
                'roa': {
                    '$first': '$roa'
                }
            }
        }, {
            '$sort': {
                'roa': -1
            }
        }
    ]
    '''
    pipeline = [
        {
            '$project': {
                'ts_code': 1,
                'ann_date': 1,
            }
        }, {
            '$group': {
                '_id': '$ts_code',
                'date': {
                    '$max': '$ann_date'
                }
            }
        }
    ]
    df = pd.DataFrame()
    for a in mongoDB.aggregate(pipeline):
        b = mongoDB.find_one({'ts_code': a['_id'], 'ann_date': a['date']})
        if (b['roa'] == None):
            b['roa'] = -999
        df = df.append(b, ignore_index=True)
    df = nu.sort_dataFrame_by_column_add_index(df, column='roa', asc=False)
    return df


def __getSortedPE__(mongoDB=DATABASE.daily_basic_tushare):
    '''

    :param mongoDB:
    :return:
    '''
    '''
    pipeline = [
        {
            '$project': {
                'ts_code': 1,
                'pe': {
                    '$ifNull': [
                        '$pe', 999
                    ]
                },
                'trade_date':1
            }
        },{
            '$group': {
                '_id': '$ts_code',
                'date': {
                    '$max': '$trade_date'
                },
                'pe': {
                    '$first': '$pe'
                }
            }
        }, {
            '$sort': {
                'pe': 1
            }
        }
    ]
    '''
    trade_date = calendar_util.get_last_x_trading_day_from_mongodb(1)
    cursor = mongoDB.find({'trade_date': str(trade_date)})
    df = pd.DataFrame(list(cursor))

    df = nu.sort_dataFrame_by_column_add_index(df, column='pe', asc=True)
    return df

def __getStockList__(mongoDB = DATABASE.stock_list):
    '''
    pipeline = [
        {
            '$project': {
                'ts_code': {
                    '$concat': [
                        '$code', '.', {
                            '$toUpper': '$sse'
                        }
                    ]
                },
                'name':"$name"
            }
        }
    ]
    '''
    stock_list = pd.DataFrame(list(mongoDB.find()))
    stock_list['ts_code'] = stock_list['code'] + stock_list['sse'].upper()
    return stock_list


def get_HighestROA_LowestPE():
    roaDf = __getSortedROA__()
    peDf = __getSortedPE__()
    stockListMap = __getStockList__()
    for stock in stockListMap['ts_code']:
        roaIndex = roaDf[roaDf['ts_code'] == stock].index.values
        peIndex = peDf[peDf['ts_code'] == stock].index.values
        if (peIndex.size > 0 and roaIndex.size > 0):
            stockListMap[stock] = int(roaIndex) + int(peIndex)

    orderedMap = OrderedDict(sorted(stockListMap.items(), key=itemgetter(1)))
    resultList = list()
    stocks = list(orderedMap.keys())

    for i in range(0, 14):
        stock = stocks[i]
        # code,roa,roa_ann_date,pe
        roaRecord = roaDf[roaDf['ts_code'] == stock]
        peRecord = peDf[peDf['ts_code'] == stock]
        roa = str(roaRecord['roa'].item())
        pe = str(peRecord['pe'].item())
        ann_date = roaRecord['ann_date'].item()
        pe_date = peRecord['trade_date'].item()
        resultList.append([stock, roa, ann_date, pe, pe_date])
    return resultList


def send_Message():
    from AY.Crius.Utils import email_func
    email_func.send_mail(__get_email_content(get_HighestROA_LowestPE()))


def __get_email_content(data):
    str = 'Subject: Data mining - highest roa and lowest pe\n'
    str = str + 'ts_code, roa, roa announcement date, pe date \r\n'
    sub = ', '
    linebreak = '\r\n'
    for i in data:
        sub = sub.join(i)
        str = str + sub + linebreak
        sub = ', '
    return str
