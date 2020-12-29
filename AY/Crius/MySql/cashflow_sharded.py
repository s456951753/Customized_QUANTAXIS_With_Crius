import sys
import time
import datetime
import logging

from sqlalchemy.exc import IntegrityError

import Utils.configuration_file_service as config_service
import Utils.DB_utils as dbUtil

import pandas as pd
import tushare as ts
from sqlalchemy import Column, String, Float, MetaData, Table, create_engine, INT

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)


def getTableMeta(year: int, metadata: MetaData) -> Table:
    """
    get corresponding table meta data.
    :param year: year of the data
    :return: a Table object representing the table structure
    """
    return Table(
        dbUtil.getTableName(year, "cashflow"), metadata,
        Column("id", INT, primary_key=True),
        Column("ts_code", String(10)),  # 股票代码
        Column("ann_date", String(8)),
        Column("f_ann_date", String(8)),
        Column("end_date", String(8)),
        Column("comp_type", String(3)),
        Column("report_type", String(3)),
        Column("net_profit", Float),  # 净利润
        Column("finan_exp", Float),  # 财务费用
        Column("c_fr_sale_sg", Float),  # 销售商品、提供劳务收到的现金
        Column("recp_tax_rends", Float),  # 收到的税费返还
        Column("n_depos_incr_fi", Float),  # 客户存款和同业存放款项净增加额
        Column("n_incr_loans_cb", Float),  # 向中央银行借款净增加额
        Column("n_inc_borr_oth_fi", Float),  # 向其他金融机构拆入资金净增加额
        Column("prem_fr_orig_contr", Float),  # 收到原保险合同保费取得的现金
        Column("n_incr_insured_dep", Float),  # 保户储金净增加额
        Column("n_reinsur_prem", Float),  # 收到再保业务现金净额
        Column("n_incr_disp_tfa", Float),  # 处置交易性金融资产净增加额
        Column("ifc_cash_incr", Float),  # 收取利息和手续费净增加额
        Column("n_incr_disp_faas", Float),  # 处置可供出售金融资产净增加额
        Column("n_incr_loans_oth_bank", Float),  # 拆入资金净增加额
        Column("n_cap_incr_repur", Float),  # 回购业务资金净增加额
        Column("c_fr_oth_operate_a", Float),  # 收到其他与经营活动有关的现金
        Column("c_inf_fr_operate_a", Float),  # 经营活动现金流入小计
        Column("c_paid_goods_s", Float),  # 购买商品、接受劳务支付的现金
        Column("c_paid_to_for_empl", Float),  # 支付给职工以及为职工支付的现金
        Column("c_paid_for_taxes", Float),  # 支付的各项税费
        Column("n_incr_clt_loan_adv", Float),  # 客户贷款及垫款净增加额
        Column("n_incr_dep_cbob", Float),  # 存放央行和同业款项净增加额
        Column("c_pay_claims_orig_inco", Float),  # 支付原保险合同赔付款项的现金
        Column("pay_handling_chrg", Float),  # 支付手续费的现金
        Column("pay_comm_insur_plcy", Float),  # 支付保单红利的现金
        Column("oth_cash_pay_oper_act", Float),  # 支付其他与经营活动有关的现金
        Column("st_cash_out_act", Float),  # 经营活动现金流出小计
        Column("n_cashflow_act", Float),  # 经营活动产生的现金流量净额
        Column("oth_recp_ral_inv_act", Float),  # 收到其他与投资活动有关的现金
        Column("c_disp_withdrwl_invest", Float),  # 收回投资收到的现金
        Column("c_recp_return_invest", Float),  # 取得投资收益收到的现金
        Column("n_recp_disp_fiolta", Float),  # 处置固定资产、无形资产和其他长期资产收回的现金净额
        Column("n_recp_disp_sobu", Float),  # 处置子公司及其他营业单位收到的现金净额
        Column("stot_inflows_inv_act", Float),  # 投资活动现金流入小计
        Column("c_pay_acq_const_fiolta", Float),  # 购建固定资产、无形资产和其他长期资产支付的现金
        Column("c_paid_invest", Float),  # 投资支付的现金
        Column("n_disp_subs_oth_biz", Float),  # 取得子公司及其他营业单位支付的现金净额
        Column("oth_pay_ral_inv_act", Float),  # 支付其他与投资活动有关的现金
        Column("n_incr_pledge_loan", Float),  # 质押贷款净增加额
        Column("stot_out_inv_act", Float),  # 投资活动现金流出小计
        Column("n_cashflow_inv_act", Float),  # 投资活动产生的现金流量净额
        Column("c_recp_borrow", Float),  # 取得借款收到的现金
        Column("proc_issue_bonds", Float),  # 发行债券收到的现金
        Column("oth_cash_recp_ral_fnc_act", Float),  # 收到其他与筹资活动有关的现金
        Column("stot_cash_in_fnc_act", Float),  # 筹资活动现金流入小计
        Column("free_cashflow", Float),  # 企业自由现金流量
        Column("c_prepay_amt_borr", Float),  # 偿还债务支付的现金
        Column("c_pay_dist_dpcp_int_exp", Float),  # 分配股利、利润或偿付利息支付的现金
        Column("incl_dvd_profit_paid_sc_ms", Float),  # 其中:子公司支付给少数股东的股利、利润
        Column("oth_cashpay_ral_fnc_act", Float),  # 支付其他与筹资活动有关的现金
        Column("stot_cashout_fnc_act", Float),  # 筹资活动现金流出小计
        Column("n_cash_flows_fnc_act", Float),  # 筹资活动产生的现金流量净额
        Column("eff_fx_flu_cash", Float),  # 汇率变动对现金的影响
        Column("n_incr_cash_cash_equ", Float),  # 现金及现金等价物净增加额
        Column("c_cash_equ_beg_period", Float),  # 期初现金及现金等价物余额
        Column("c_cash_equ_end_period", Float),  # 期末现金及现金等价物余额
        Column("c_recp_cap_contrib", Float),  # 吸收投资收到的现金
        Column("incl_cash_rec_saims", Float),  # 其中:子公司吸收少数股东投资收到的现金
        Column("uncon_invest_loss", Float),  # 未确认投资损失
        Column("prov_depr_assets", Float),  # 加:资产减值准备
        Column("depr_fa_coga_dpba", Float),  # 固定资产折旧、油气资产折耗、生产性生物资产折旧
        Column("amort_intang_assets", Float),  # 无形资产摊销
        Column("lt_amort_deferred_exp", Float),  # 长期待摊费用摊销
        Column("decr_deferred_exp", Float),  # 待摊费用减少
        Column("incr_acc_exp", Float),  # 预提费用增加
        Column("loss_disp_fiolta", Float),  # 处置固定、无形资产和其他长期资产的损失
        Column("loss_scr_fa", Float),  # 固定资产报废损失
        Column("loss_fv_chg", Float),  # 公允价值变动损失
        Column("invest_loss", Float),  # 投资损失
        Column("decr_def_inc_tax_assets", Float),  # 递延所得税资产减少
        Column("incr_def_inc_tax_liab", Float),  # 递延所得税负债增加
        Column("decr_inventories", Float),  # 存货的减少
        Column("decr_oper_payable", Float),  # 经营性应收项目的减少
        Column("incr_oper_payable", Float),  # 经营性应付项目的增加
        Column("others", Float),  # 其他
        Column("im_net_cashflow_oper_act", Float),  # 经营活动产生的现金流量净额(间接法)
        Column("conv_debt_into_cap", Float),  # 债务转为资本
        Column("conv_copbonds_due_within_1y", Float),  # 一年内到期的可转换公司债券
        Column("fa_fnc_leases", Float),  # 融资租入固定资产
        Column("end_bal_cash", Float),  # 现金的期末余额
        Column("beg_bal_cash", Float),  # 减:现金的期初余额
        Column("end_bal_cash_equ", Float),  # 加:现金等价物的期末余额
        Column("beg_bal_cash_equ", Float),  # 减:现金等价物的期初余额
        Column("im_n_incr_cash_equ", Float),  # 现金及现金等价物净增加额(间接法)
        Column("update_flag", Float)  # 更新标识
    )

def get_ts_code(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code from stock_basic', engine)

def get_ts_code_and_list_date(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code,list_date from stock_basic', engine)


def update_bulk_cashflow_by_period_and_ts_code(base_name, engine, pro, codes, start_date, end_date, retry_count=3,
                                             pause=2):
    coverage = dbUtil.getTableRange(base_name="", start_date=start_date, end_date=end_date)
    for i in coverage:
        for rownum in range(0, len(codes)):
            logger.debug("started processing data for " + codes.iloc[rownum]['ts_code'] + " for period " + i)
            if (int(codes.iloc[rownum]['list_date'][0:4]) <= int(i[1:5]) or int(
                    codes.iloc[rownum]['list_date'][0:4]) <= int(i[6:10])):
                try:
                    to_insert = pro.cashflow_vip(ts_code=codes.iloc[rownum]['ts_code'], start_date=i[1:5] + '0101',
                                               end_date=i[6:10] + '1231')
                    logger.debug("start inserting data into DB")
                    to_insert.to_sql(base_name + i, engine, if_exists='append', index=False)
                    logger.debug("end inserting data into DB")
                except Exception as e:
                    logger.error(e)
                    logger.error(
                        "error processing data for range " + str(i) + " for code " + codes.iloc[rownum]['ts_code'])


def update_bulk_cashflow_by_ts_code_and_insert_by_year(base_name, engine, pro, codes, sharding_column, failed_count=0,
                                                     failed_tolerent=3):
    failed = []
    for code in codes['ts_code']:
        logger.debug("started processing data for " + code)
        try:
            to_insert = pro.cashflow_vip(ts_code=code)
            logger.debug("start inserting data into DB")
            distinct_years = set(to_insert[sharding_column].str[0:4])
            for year in distinct_years:
                year_section = to_insert[to_insert[sharding_column].str[0:4] == year]
                if (year == None):
                    year = 9999
                    year_section = to_insert[pd.isna(to_insert[sharding_column]) == True]
                year_section.to_sql(dbUtil.getTableName(int(year), base_name=base_name), engine, if_exists='append',
                                    index=False)
            logger.debug("end inserting data into DB")
        except Exception as e:
            failed.append(code)
            logger.error(e)
            logger.error("error processing data for code " + code)
    if (failed_count < failed_tolerent):
        logger.warning("retrying now.")
        failed_count = failed_count + 1
        update_bulk_cashflow_by_ts_code_and_insert_by_year(base_name=base_name, engine=engine, pro=pro,
                                                         codes=pd.DataFrame(failed, columns=['ts_code']),
                                                         sharding_column=sharding_column,
                                                         failed_count=failed_count)
    else:
        logger.error("the below code has failed after maximum attempts. " + ','.join(failed))

logger = logging.getLogger('cashflow_sharded')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setStream(sys.stdout)
fi = logging.FileHandler(filename="../engine.log")

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fi.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
logger.addHandler(fi)

engine = create_engine(config_service.getDefaultDB())
conn = engine.connect()

metadata = MetaData()

years = dbUtil.getYears()
for i in years.keys():
    getTableMeta(i, metadata)

metadata.create_all(engine)

# df = get_ts_code_and_list_date(engine)
df = get_ts_code(engine)
# update_bulk_cashflow_by_period_and_ts_code(base_name='cashflow', engine=engine, pro=pro, codes=df, start_date='19950101',
# end_date=datetime.date.today().strftime("%Y%m%d"))
update_bulk_cashflow_by_ts_code_and_insert_by_year(base_name='cashflow', engine=engine, pro=pro, codes=df,
                                                 sharding_column='f_ann_date')
