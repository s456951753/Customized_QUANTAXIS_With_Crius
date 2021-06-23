from typing import Dict

import AY.Crius.Utils.trading_calendar_utils as calendar_util
import pandas as pd
from QUANTAXIS.QAUtil import DATABASE
import AY.Crius.Utils.numeric_utils as numeric_utils
import AY.Crius.Utils.trading_calendar_utils as trading_calendar_utils

CASH_FLOW_TYPE_NAME = 'cash_flow'
BALANCE_SHEET_TYPE_NAME = 'balance_sheet'
FINANCIAL_INDICATOR_TYPE_NAME = 'financial_indicator'

TABLE_LATEST_DATE_COLUMN_MAPPING = {CASH_FLOW_TYPE_NAME: 'f_ann_date',
                                    BALANCE_SHEET_TYPE_NAME: 'f_ann_date',
                                    FINANCIAL_INDICATOR_TYPE_NAME: 'ann_date'}


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


def get_latest_cash_flow_table(mongoDB=DATABASE.cash_flow):
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


def get_stock_list(mongoDB=DATABASE.stock_list, version='crius'):
    df = pd.DataFrame(list(mongoDB.find()))
    if (version == 'crius'):
        return df
    df = df.assign(ts_code=lambda x: x.code + '.' + x.sse)
    df['ts_code'] = df['ts_code'].str.upper()
    return df


def rename_adding_suffix_with_exceptions(df, suffix, exception_column_name):
    return df.rename(columns=lambda x: x + suffix if x != exception_column_name else x)


def add_necessary_data(start_date=None, mongoDB=DATABASE, auto_detect=False):
    import QUANTAXIS.QASU.save_tushare as st

    # drop and rebuild
    __coll_stock_list = mongoDB.stock_list
    __coll_trade_date = mongoDB.trade_date

    # append daily
    __coll_daily_basic = mongoDB.daily_basic

    # report like tables
    __coll_balance_sheet = mongoDB.balance_sheet
    __coll_finance_indicator = mongoDB.finance_inicator
    __coll_cash_flow = mongoDB.cash_flow

    # initially adding all data
    if (start_date is None):
        st.QA_SU_save_trade_date_all()
        st.QA_SU_save_stock_list('tushare')


def rank_dataframe_columns_adding_index(df: pd.DataFrame, ranking_setup: Dict, base_name='ts_code'):
    """

    Args:
        df: the dataframe to be sorted
        ranking_setup: a setup for ranking in the form of Dict[str, bool]
        base_name: the column used to merge the dataframe back. Default as ts_code
    Returns:
        the sorted df with index added
    """

    for column in ranking_setup.keys():
        ranked_column_name = str(column) + "_rank"
        to_rank = df[[base_name, column]]
        ranked = numeric_utils.sort_dataFrame_by_column_add_index(df=to_rank, column=column, asc=ranking_setup[column])
        ranked = ranked.assign(ranked_column_name=lambda x: x.index + 1)
        ranked = ranked.rename(columns={base_name: base_name, column: column, "ranked_column_name": ranked_column_name})
        ranked = ranked.drop(columns=column)
        df = df.merge(right=ranked, on=base_name)
    return df


def get_daily_data_to_db():
    '''
    Runs every day to pull data from tushare
    Not suitable to be used as bulky data pulling
    Returns:

    '''
    import QUANTAXIS.QASU.save_tushare as st

    st.QA_SU_save_stock_list()

    table_date_mapping = get_latest_local_stored_date()
    st.QA_SU_save_report_type_table(table_type=FINANCIAL_INDICATOR_TYPE_NAME, start_ann_date=int(table_date_mapping[FINANCIAL_INDICATOR_TYPE_NAME])+1)
    st.QA_SU_save_report_type_table(table_type=CASH_FLOW_TYPE_NAME, start_ann_date=int(table_date_mapping[CASH_FLOW_TYPE_NAME])+1)
    st.QA_SU_save_report_type_table(table_type=BALANCE_SHEET_TYPE_NAME, start_ann_date=int(table_date_mapping[BALANCE_SHEET_TYPE_NAME])+1)

    st.QA_SU_save_daily_basic()


def get_latest_local_stored_date(table_name=None, client=DATABASE):
    tables = []
    latest_dates = {}
    if (table_name is None):
        tables = {FINANCIAL_INDICATOR_TYPE_NAME, CASH_FLOW_TYPE_NAME, BALANCE_SHEET_TYPE_NAME}
    else:
        tables[0] = table_name
    for ta in tables:
        if (ta == CASH_FLOW_TYPE_NAME):
            __coll = client.cash_flow
        elif (ta == BALANCE_SHEET_TYPE_NAME):
            __coll = client.balance_sheet
        elif (ta == FINANCIAL_INDICATOR_TYPE_NAME):
            __coll = client.finacial_indicator
        pipeline = [
            {
                '$sort': {
                    TABLE_LATEST_DATE_COLUMN_MAPPING[ta]: -1
                }
            }, {
                '$limit': 1
            }
        ]
        for a in __coll.aggregate(pipeline):
            latest_dates[ta] = a[TABLE_LATEST_DATE_COLUMN_MAPPING[ta]]

    return latest_dates
