import pandas as pd
import AY.Crius.Utils.tushare_service as tushare_service
from QUANTAXIS.QAUtil import DATABASE
import AY.Crius.Utils.Technical_indicator as tech_indicator
import AY.Crius.Utils.trading_calendar_utils as calendar_util
import AY.Crius.Utils.numeric_utils as nu
import AY.Crius.Utils.data_mining_utils as miner_util


def mine():
    daily_basic_df = pd.DataFrame(miner_util.get_latest_daily_basic_table()[[
        "ts_code", "close", "trade_date", "turnover_rate", "total_mv", "pb", "ps", "pe"]])
    financial_indicator_df = pd.DataFrame(miner_util.get_latest_finacial_indicator_table()[[
        "ts_code", "ann_date", "ocfps", "profit_dedt", "q_sales_yoy", "invturn_days"]])
    balance_sheet_df = pd.DataFrame(miner_util.get_latest_balance_sheet_table()[[
        "ts_code", "f_ann_date", "update_flag", "total_assets", "cash_reser_cb", "depos_in_oth_bfi"]])
    stock_list_df = pd.DataFrame(miner_util.get_stock_list()[["ts_code", "name"]])

    for code in stock_list_df:
        df_balance_sheet_code = balance_sheet_df[balance_sheet_df.ts_code == code]
        if df_balance_sheet_code.size > 1:
            balance_sheet_df.drop(
                balance_sheet_df[(balance_sheet_df.ts_code == code) & (balance_sheet_df.update_flag == 0)].index)
    df = daily_basic_df.merge(right=stock_list_df, on='ts_code').merge(right=financial_indicator_df,
                                                                       on="ts_code").merge(right=balance_sheet_df,
                                                                                           on="ts_code")

    return df


def send_Message():
    from AY.Crius.Utils import email_func
    email_func.send_mail(mine().to_html)


send_Message()
