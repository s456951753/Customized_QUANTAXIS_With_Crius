import pandas as pd
import AY.Crius.Utils.tushare_service as tushare_service
from QUANTAXIS.QAUtil import DATABASE
import AY.Crius.Utils.Technical_indicator as tech_indicator
import AY.Crius.Utils.trading_calendar_utils as calendar_util
import AY.Crius.Utils.numeric_utils as nu
import AY.Crius.Utils.data_mining_utils as miner_util


def mine():
    daily_basic_df = miner_util.get_latest_daily_basic_table()
    financial_indicator_df = miner_util.get_latest_finacial_indicator_table()
    balance_sheet_df = miner_util.get_latest_balance_sheet_table()
    stock_list_df = pd.DataFrame(miner_util.get_stock_list()["ts_code", "name"])

    df = pd.DataFrame(daily_basic_df[["ts_code", "close", "trade_date", "turnover_rate", "total_mv"]])
    df = df.merge(right=stock_list_df, on='ts_code')

    return df


mine()
