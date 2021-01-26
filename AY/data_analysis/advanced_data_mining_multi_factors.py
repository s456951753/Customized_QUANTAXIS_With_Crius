import pandas as pd
import AY.Crius.Utils.data_mining_utils as miner_util


def mine():
    daily_basic_df = pd.DataFrame(miner_util.get_latest_daily_basic_table()[[
        "ts_code", "close", "trade_date", "turnover_rate", "total_mv", "pb", "ps", "pe"]])
    financial_indicator_df = pd.DataFrame(miner_util.get_latest_finacial_indicator_table()[[
        "ts_code", "ann_date", "ocfps", "profit_dedt", "q_sales_yoy", "invturn_days"]])
    balance_sheet_df = pd.DataFrame(miner_util.get_latest_balance_sheet_table()[[
        "ts_code", "f_ann_date", "update_flag", "total_assets", "cash_reser_cb", "depos_in_oth_bfi", "end_date"]])
    stock_list_df = pd.DataFrame(miner_util.get_stock_list()[["ts_code", "name"]])
    cash_flow_df = pd.DataFrame(
        miner_util.get_latest_cash_flow_table()[["ts_code", "f_ann_date", "end_bal_cash", "update_flag", "end_date"]])
    for code in stock_list_df["ts_code"]:
        df_balance_sheet_code = balance_sheet_df[balance_sheet_df.ts_code == code]
        df_cash_flow_code = cash_flow_df[cash_flow_df.ts_code == code]
        balance_sheet_max_end_date = df_balance_sheet_code[
            df_balance_sheet_code['end_date'] == df_balance_sheet_code['end_date'].max()]
        cash_flow_max_end_date = df_cash_flow_code[df_cash_flow_code['end_date'] == df_cash_flow_code['end_date'].max()]
        if len(df_balance_sheet_code.index) > 1:
            balance_sheet_df = balance_sheet_df.drop(
                balance_sheet_df[(balance_sheet_df.ts_code == code) & (balance_sheet_df.update_flag == '0') & (
                            balance_sheet_df.end_date < balance_sheet_max_end_date)].index)
        if len(df_cash_flow_code.index) > 1:
            cash_flow_df = cash_flow_df.drop(
                cash_flow_df[(cash_flow_df.ts_code == code) & (cash_flow_df.update_flag == '0')& (
                            cash_flow_df.end_date < cash_flow_max_end_date)].index)

    balance_sheet_df = miner_util.rename_adding_suffix_with_exceptions(df=balance_sheet_df, suffix='_balance_sheet',
                                                                       exception_column_name='ts_code')
    cash_flow_df = miner_util.rename_adding_suffix_with_exceptions(df=cash_flow_df, suffix='_cash_flow',
                                                                   exception_column_name='ts_code')
    df = daily_basic_df.merge(right=stock_list_df,
                              on='ts_code').merge(right=financial_indicator_df,
                                                  on="ts_code").merge(right=balance_sheet_df,
                                                                      on="ts_code").merge(
        right=cash_flow_df, on="ts_code")
    # data cleaning

    # data analysis
    df = df.assign(cash_to_market_cap=lambda x: (x.end_bal_cash_cash_flow) / x.total_mv)
    df = df.assign(price_to_op_cash_flow = lambda x:x.close / x.ocfps)
    df = df.assign(gross_profit_over_assets = lambda x: x.profit_dedt/x.total_assets_balance_sheet)

    df.to_csv("test.csv", sep='\t', encoding='utf-8')
    return df


def send_Message():
    from AY.Crius.Utils import email_func
    email_func.send_mail(mine().to_html())


send_Message()
