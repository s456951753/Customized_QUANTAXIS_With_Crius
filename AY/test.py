import QUANTAXIS.QASU.save_tushare as st
import AY.Crius.Utils.data_mining_utils as data_mining_utils
from QUANTAXIS import (QA_SU_save_etf_day, QA_SU_save_index_day, QA_SU_save_stock_min,
                       QA_SU_save_stock_block, QA_SU_save_stock_day,QA_SU_save_etf_min,
                       QA_SU_save_stock_list, QA_SU_save_stock_xdxr,
                       QA_util_log_info)

#st.QA_SU_save_trade_date_all()
#st.QA_SU_save_stock_info_tushare()
st.QA_SU_save_stock_list()
#st.QA_SU_save_finacial_inicator_data()
#for i in ['20210106','20210107','20210108']:
#st.QA_SU_save_daily_basic()
#st.QA_SU_save_balance_sheet()
#st.QA_SU_save_report_type_table(table_type=data_mining_utils.CASH_FLOW_TYPE_NAME)
#QA_SU_save_stock_list('tushare')