import QUANTAXIS.QASU.save_tushare as st
import AY.Crius.Utils.data_mining_utils as data_mining_utils
import AY.Crius.Utils.trading_calendar_utils as trading_calendar_utils
from QUANTAXIS import (QA_SU_save_etf_day, QA_SU_save_index_day, QA_SU_save_stock_min,
                       QA_SU_save_stock_block, QA_SU_save_stock_day,QA_SU_save_etf_min,
                       QA_SU_save_stock_list, QA_SU_save_stock_xdxr,
                       QA_util_log_info)
import pandas as pd

#st.QA_SU_save_trade_date_all()
#QA_SU_save_stock_list('tushare')
'''
print(trading_calendar_utils.get_yesterday_as_str())
print(trading_calendar_utils.get_days_between())
print(trading_calendar_utils.get_days_between(start_date='20200105'))
print(trading_calendar_utils.get_days_between(end_date='19930105'))
print(trading_calendar_utils.get_days_between(start_date='20200105',end_date='20200105'))
'''
print(trading_calendar_utils.get_days_between(start_date='20210101'))

#st.QA_SU_save_report_type_table(table_type=data_mining_utils.FINANCIAL_INDICATOR_TYPE_NAME, start_ann_date=20210417)
##st.QA_SU_save_report_type_table(table_type=data_mining_utils.CASH_FLOW_TYPE_NAME, start_ann_date=20210417)
#st.QA_SU_save_report_type_table(table_type=data_mining_utils.BALANCE_SHEET_TYPE_NAME, start_ann_date=20210417)
#st.QA_SU_save_daily_basic()
#data_mining_utils.get_daily_data_to_db()
'''
df = pd.DataFrame({
     'col1' : ['A', 'B', 'C', 'D','E','F'],
     'col2' : [2, 1, 9, 8, 7, 4],
     'col3': [0, 1, 9, 4, 2, 3],
 })
ranking_setup = {"col2": True,
                 "col3": False}
df = data_mining_utils.rank_dataframe_columns_adding_index(df=df,ranking_setup=ranking_setup,base_name="col1")
print(df)
'''
#st.QA_SU_save_stock_info_tushare()
#st.QA_SU_save_stock_list()
#st.QA_SU_save_finacial_inicator_data()
#for i in ['20210106','20210107','20210108']:
#
#st.QA_SU_save_balance_sheet()
#st.QA_SU_save_report_type_table(table_type=data_mining_utils.CASH_FLOW_TYPE_NAME)

