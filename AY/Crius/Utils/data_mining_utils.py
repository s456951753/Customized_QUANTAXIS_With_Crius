import AY.Crius.Utils.trading_calendar_utils as calendar_util
import pandas as pd
from QUANTAXIS.QAUtil import DATABASE


def get_latest_daily_basic_table(mongoDB=DATABASE.daily_basic_tushare):
    trade_date = calendar_util.get_last_x_trading_day_from_mongodb(1)
    cursor = mongoDB.find({'trade_date': str(trade_date)})
    df = pd.DataFrame(list(cursor))

    return df


def get_latest_finacial_indicator_table(mongoDB=DATABASE.finacial_indicator):
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
        df = df.append(b, ignore_index=True)
    return df


def get_latest_balance_sheet_table(mongoDB=DATABASE.balance_sheet):
    pipeline = [
        {
            '$project': {
                'ts_code': 1,
                'f_ann_date': 1,
            }
        }, {
            '$group': {
                '_id': '$ts_code',
                'date': {
                    '$max': '$f_ann_date'
                }
            }
        }
    ]
    df = pd.DataFrame()
    for a in mongoDB.aggregate(pipeline):
        b = mongoDB.find({'ts_code': a['_id'], 'f_ann_date': a['date']})
        for c in b:
            df = df.append(c, ignore_index=True)
    return df


def get_stock_list(mongoDB=DATABASE.stock_list):
    df = pd.DataFrame(list(mongoDB.find()))
    df = df.assign(ts_code=lambda x: x.code + '.' + x.sse)
    df['ts_code'] = df['ts_code'].str.upper()
    return df
