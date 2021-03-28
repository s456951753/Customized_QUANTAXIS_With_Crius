import pandas as pd
import AY.Crius.Utils.data_mining_utils as miner_util


def mine():
    daily_basic_df = pd.DataFrame(miner_util.get_latest_daily_basic_table()[[
        "ts_code", "close", "trade_date", "turnover_rate", "total_mv", "pb", "ps", "pe"]])
    # removed invturn_days field cause it's not a default available filed for financial indicator table
    financial_indicator_df = pd.DataFrame(miner_util.get_latest_finacial_indicator_table()[[
        "ts_code", "ann_date", "ocfps", "profit_dedt", "q_sales_yoy"]])
    balance_sheet_df = pd.DataFrame(miner_util.get_latest_balance_sheet_table()[[
        "ts_code", "f_ann_date", "update_flag", "total_assets", "cash_reser_cb", "depos_in_oth_bfi", "end_date"]])
    stock_list_df = pd.DataFrame(miner_util.get_stock_list()[["ts_code", "name"]])
    cash_flow_df = pd.DataFrame(
        miner_util.get_latest_cash_flow_table()[["ts_code", "f_ann_date", "end_bal_cash", "update_flag", "end_date"]])
    for code in stock_list_df["ts_code"]:
        # clean data for balance sheet
        df_balance_sheet_code = balance_sheet_df[balance_sheet_df.ts_code == code]
        if (len(df_balance_sheet_code.index)<=0):
            # skip cause there's no data for that code
            continue
        elif(len(df_balance_sheet_code.index)>1):
            # we got more than 1 record for the same f_ann_date. filter by end date now
            d=df_balance_sheet_code['end_date'].max()
            if len(df_balance_sheet_code.index) > 1:
                balance_sheet_df = balance_sheet_df.drop(
                    balance_sheet_df[(balance_sheet_df.ts_code == code) & (balance_sheet_df.end_date < d)].index)
                # clear ones that are replaced by updates i.e. update_flag = 0
            if len(balance_sheet_df[balance_sheet_df.ts_code == code]) > 1:
                balance_sheet_df = balance_sheet_df.drop(
                    balance_sheet_df[(balance_sheet_df.ts_code == code) & (balance_sheet_df.update_flag == '0')].index)
        # otherwise we are happy cause we got 1 record

        # clean data for cash flow
        df_cash_flow_code = cash_flow_df[cash_flow_df.ts_code == code]
        if (len(df_cash_flow_code.index)<=0):
            continue
        elif(len(df_cash_flow_code.index)>1):
            d = df_cash_flow_code['end_date'].max()
            # clear ones that not covering the correct period
            if len(df_cash_flow_code.index) > 1:
                cash_flow_df = cash_flow_df.drop(
                    cash_flow_df[(cash_flow_df.ts_code == code) & (cash_flow_df.end_date < d)].index)
            # clear ones that are replaced by updates i.e. update_flag = 0
            if len(cash_flow_df[cash_flow_df.ts_code == code]) > 1:
                cash_flow_df = cash_flow_df.drop(
                    cash_flow_df[(cash_flow_df.ts_code == code) & (cash_flow_df.update_flag == '0')].index)


    balance_sheet_df = miner_util.rename_adding_suffix_with_exceptions(df=balance_sheet_df, suffix='_balance_sheet',
                                                                       exception_column_name='ts_code')
    cash_flow_df = miner_util.rename_adding_suffix_with_exceptions(df=cash_flow_df, suffix='_cash_flow',
                                                                   exception_column_name='ts_code')
    df = daily_basic_df.merge(right=stock_list_df,
                              on='ts_code').merge(right=financial_indicator_df,
                                                  on="ts_code").merge(right=balance_sheet_df,
                                                                      on="ts_code").merge(
        right=cash_flow_df, on="ts_code")

    # clean data - drop any code that has 0 total_mv, ocfps or total_assets
    df=df.drop(df[df.total_mv * df.ocfps * df.total_assets_balance_sheet==0].index)
    # clean data -any code that has NA pe
    df = df.drop(df[df.pe.isna() == True].index)
    # data analysis column

    df = df.assign(cash_to_market_cap=lambda x: (x.end_bal_cash_cash_flow) / x.total_mv)
    df = df.assign(price_to_op_cash_flow = lambda x:x.close / x.ocfps)
    df = df.assign(gross_profit_over_assets = lambda x: x.profit_dedt/x.total_assets_balance_sheet)

    df.to_csv("test.csv", sep='\t', encoding='utf-8')
    return df


def send_Message():
    from AY.Crius.Utils import email_func
    email_func.send_mail(mine())



send_Message()
