from collections import OrderedDict
from operator import itemgetter

import QUANTAXIS.QAFetch.QAQuery as dbQuery
from QUANTAXIS.QAUtil import DATABASE
import pandas as pd
from pandas.io.json import json_normalize
import AY.Crius.Utils.configuration_file_service as config_service

def __getSortedROA__(mongoDB = DATABASE.finacial_indicator):
    '''
    get roa of stocks, sorted by roa from high to low. Using data: tushare's financial indicator

    :param mongoDB:
    :return:
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
    df = pd.DataFrame()
    for a in mongoDB.aggregate(pipeline):
        df=df.append(a,ignore_index=True)
    return df

def __getSortedPE__(mongoDB = DATABASE.stock_info_tushare):
    pipeline = [
        {
            '$project': {
                'code': 1,
                'pe': {
                    '$ifNull': [
                        '$pe', 999
                    ]
                }
            }
        }, {
            '$sort': {
                'pe': 1
            }
        }
    ]
    df = pd.DataFrame()
    for a in mongoDB.aggregate(pipeline):
        if(a['pe']>0):
            df=df.append(a,ignore_index=True)
    return df

def __getStockList__(mongoDB = DATABASE.stock_list):
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
    map = {}
    for a in mongoDB.aggregate(pipeline):
        map[a['ts_code']]=9999
    return map

roaDf = __getSortedROA__()
peDf = __getSortedPE__()
stockListMap = __getStockList__()
for stock in stockListMap.keys():
    peCode = stock[0:6]
    roaIndex = roaDf[roaDf['_id']==stock].index.values
    peIndex =  peDf[peDf['code']==peCode].index.values
    if(peIndex.size>0 and roaIndex.size>0):
        stockListMap[stock] = int(roaIndex)+int(peIndex)

orderedMap = OrderedDict(sorted(stockListMap.items(), key=itemgetter(1)))
resultList = list()
for i in range(0,14):
    stock=list(orderedMap.keys())[i]
    peCode = stock[0:6]
    # code,roa
    resultList.append([stock,roaDf[roaDf['_id']==stock]['roa'],roaDf[roaDf['_id']==stock]['date'],peDf[peDf['code']==peCode]['pe']])
