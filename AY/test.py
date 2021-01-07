import QUANTAXIS.QASU.save_tushare as st
import AY.Crius.Utils.data_mining_utils as data_mining_utils

#st.QA_SU_save_trade_date_all()
#st.QA_SU_save_stock_info_tushare()
#st.QA_SU_save_stock_list()
#st.QA_SU_save_finacial_inicator_data()
#st.QA_SU_save_daily_basic()
#st.QA_SU_save_balance_sheet()
st.QA_SU_save_report_type_table(table_type=data_mining_utils.CASH_FLOW_TYPE_NAME)