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
        dbUtil.getTableName(year, "balancesheet"), metadata,
        Column("ts_code", String(10)),  # 股票代码
        Column("ann_date", String(8)),
        Column("f_ann_date", String(8)),
        Column("end_date", String(8)),
        Column("report_type", String(3)),
        Column("comp_type", String(3)),
        Column("total_share", Float),  # 期末总股本
        Column("cap_rese", Float),  # 资本公积金
        Column("undistr_porfit", Float),  # 未分配利润
        Column("surplus_rese", Float),  # 盈余公积金
        Column("special_rese", Float),  # 专项储备
        Column("money_cap", Float),  # 货币资金
        Column("trad_asset", Float),  # 交易性金融资产
        Column("notes_receiv", Float),  # 应收票据
        Column("accounts_receiv", Float),  # 应收账款
        Column("oth_receiv", Float),  # 其他应收款
        Column("prepayment", Float),  # 预付款项
        Column("div_receiv", Float),  # 应收股利
        Column("int_receiv", Float),  # 应收利息
        Column("inventories", Float),  # 存货
        Column("amor_exp", Float),  # 待摊费用
        Column("nca_within_1y", Float),  # 一年内到期的非流动资产
        Column("sett_rsrv", Float),  # 结算备付金
        Column("loanto_oth_bank_fi", Float),  # 拆出资金
        Column("premium_receiv", Float),  # 应收保费
        Column("reinsur_receiv", Float),  # 应收分保账款
        Column("reinsur_res_receiv", Float),  # 应收分保合同准备金
        Column("pur_resale_fa", Float),  # 买入返售金融资产
        Column("oth_cur_assets", Float),  # 其他流动资产
        Column("total_cur_assets", Float),  # 流动资产合计
        Column("fa_avail_for_sale", Float),  # 可供出售金融资产
        Column("htm_invest", Float),  # 持有至到期投资
        Column("lt_eqt_invest", Float),  # 长期股权投资
        Column("invest_real_estate", Float),  # 投资性房地产
        Column("time_deposits", Float),  # 定期存款
        Column("oth_assets", Float),  # 其他资产
        Column("lt_rec", Float),  # 长期应收款
        Column("fix_assets", Float),  # 固定资产
        Column("cip", Float),  # 在建工程
        Column("const_materials", Float),  # 工程物资
        Column("fixed_assets_disp", Float),  # 固定资产清理
        Column("produc_bio_assets", Float),  # 生产性生物资产
        Column("oil_and_gas_assets", Float),  # 油气资产
        Column("intan_assets", Float),  # 无形资产
        Column("r_and_d", Float),  # 研发支出
        Column("goodwill", Float),  # 商誉
        Column("lt_amor_exp", Float),  # 长期待摊费用
        Column("defer_tax_assets", Float),  # 递延所得税资产
        Column("decr_in_disbur", Float),  # 发放贷款及垫款
        Column("oth_nca", Float),  # 其他非流动资产
        Column("total_nca", Float),  # 非流动资产合计
        Column("cash_reser_cb", Float),  # 现金及存放中央银行款项
        Column("depos_in_oth_bfi", Float),  # 存放同业和其它金融机构款项
        Column("prec_metals", Float),  # 贵金属
        Column("deriv_assets", Float),  # 衍生金融资产
        Column("rr_reins_une_prem", Float),  # 应收分保未到期责任准备金
        Column("rr_reins_outstd_cla", Float),  # 应收分保未决赔款准备金
        Column("rr_reins_lins_liab", Float),  # 应收分保寿险责任准备金
        Column("rr_reins_lthins_liab", Float),  # 应收分保长期健康险责任准备金
        Column("refund_depos", Float),  # 存出保证金
        Column("ph_pledge_loans", Float),  # 保户质押贷款
        Column("refund_cap_depos", Float),  # 存出资本保证金
        Column("indep_acct_assets", Float),  # 独立账户资产
        Column("client_depos", Float),  # 其中：客户资金存款
        Column("client_prov", Float),  # 其中：客户备付金
        Column("transac_seat_fee", Float),  # 其中:交易席位费
        Column("invest_as_receiv", Float),  # 应收款项类投资
        Column("total_assets", Float),  # 资产总计
        Column("lt_borr", Float),  # 长期借款
        Column("st_borr", Float),  # 短期借款
        Column("cb_borr", Float),  # 向中央银行借款
        Column("depos_ib_deposits", Float),  # 吸收存款及同业存放
        Column("loan_oth_bank", Float),  # 拆入资金
        Column("trading_fl", Float),  # 交易性金融负债
        Column("notes_payable", Float),  # 应付票据
        Column("acct_payable", Float),  # 应付账款
        Column("adv_receipts", Float),  # 预收款项
        Column("sold_for_repur_fa", Float),  # 卖出回购金融资产款
        Column("comm_payable", Float),  # 应付手续费及佣金
        Column("payroll_payable", Float),  # 应付职工薪酬
        Column("taxes_payable", Float),  # 应交税费
        Column("int_payable", Float),  # 应付利息
        Column("div_payable", Float),  # 应付股利
        Column("oth_payable", Float),  # 其他应付款
        Column("acc_exp", Float),  # 预提费用
        Column("deferred_inc", Float),  # 递延收益
        Column("st_bonds_payable", Float),  # 应付短期债券
        Column("payable_to_reinsurer", Float),  # 应付分保账款
        Column("rsrv_insur_cont", Float),  # 保险合同准备金
        Column("acting_trading_sec", Float),  # 代理买卖证券款
        Column("acting_uw_sec", Float),  # 代理承销证券款
        Column("non_cur_liab_due_1y", Float),  # 一年内到期的非流动负债
        Column("oth_cur_liab", Float),  # 其他流动负债
        Column("total_cur_liab", Float),  # 流动负债合计
        Column("bond_payable", Float),  # 应付债券
        Column("lt_payable", Float),  # 长期应付款
        Column("specific_payables", Float),  # 专项应付款
        Column("estimated_liab", Float),  # 预计负债
        Column("defer_tax_liab", Float),  # 递延所得税负债
        Column("defer_inc_non_cur_liab", Float),  # 递延收益-非流动负债
        Column("oth_ncl", Float),  # 其他非流动负债
        Column("total_ncl", Float),  # 非流动负债合计
        Column("depos_oth_bfi", Float),  # 同业和其它金融机构存放款项
        Column("deriv_liab", Float),  # 衍生金融负债
        Column("depos", Float),  # 吸收存款
        Column("agency_bus_liab", Float),  # 代理业务负债
        Column("oth_liab", Float),  # 其他负债
        Column("prem_receiv_adva", Float),  # 预收保费
        Column("depos_received", Float),  # 存入保证金
        Column("ph_invest", Float),  # 保户储金及投资款
        Column("reser_une_prem", Float),  # 未到期责任准备金
        Column("reser_outstd_claims", Float),  # 未决赔款准备金
        Column("reser_lins_liab", Float),  # 寿险责任准备金
        Column("reser_lthins_liab", Float),  # 长期健康险责任准备金
        Column("indept_acc_liab", Float),  # 独立账户负债
        Column("pledge_borr", Float),  # 其中:质押借款
        Column("indem_payable", Float),  # 应付赔付款
        Column("policy_div_payable", Float),  # 应付保单红利
        Column("total_liab", Float),  # 负债合计
        Column("treasury_share", Float),  # 减:库存股
        Column("ordin_risk_reser", Float),  # 一般风险准备
        Column("forex_differ", Float),  # 外币报表折算差额
        Column("invest_loss_unconf", Float),  # 未确认的投资损失
        Column("minority_int", Float),  # 少数股东权益
        Column("total_hldr_eqy_exc_min_int", Float),  # 股东权益合计(不含少数股东权益)
        Column("total_hldr_eqy_inc_min_int", Float),  # 股东权益合计(含少数股东权益)
        Column("total_liab_hldr_eqy", Float),  # 负债及股东权益总计
        Column("lt_payroll_payable", Float),  # 长期应付职工薪酬
        Column("oth_comp_income", Float),  # 其他综合收益
        Column("oth_eqt_tools", Float),  # 其他权益工具
        Column("oth_eqt_tools_p_shr", Float),  # 其他权益工具(优先股)
        Column("lending_funds", Float),  # 融出资金
        Column("acc_receivable", Float),  # 应收款项
        Column("st_fin_payable", Float),  # 应付短期融资款
        Column("payables", Float),  # 应付款项
        Column("hfs_assets", Float),  # 持有待售的资产
        Column("hfs_sales", Float),  # 持有待售的负债
        Column("update_flag", Float)  # 更新标识
    )

def get_ts_code(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code from stock_basic', engine)

def get_ts_code_and_list_date(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code,list_date from stock_basic', engine)


def update_bulk_balancesheet_by_period_and_ts_code(base_name, engine, pro, codes, start_date, end_date, retry_count=3,
                                             pause=2):
    coverage = dbUtil.getTableRange(base_name="", start_date=start_date, end_date=end_date)
    for i in coverage:
        for rownum in range(0, len(codes)):
            logger.debug("started processing data for " + codes.iloc[rownum]['ts_code'] + " for period " + i)
            if (int(codes.iloc[rownum]['list_date'][0:4]) <= int(i[1:5]) or int(
                    codes.iloc[rownum]['list_date'][0:4]) <= int(i[6:10])):
                try:
                    to_insert = pro.balancesheet_vip(ts_code=codes.iloc[rownum]['ts_code'], start_date=i[1:5] + '0101',
                                               end_date=i[6:10] + '1231')
                    logger.debug("start inserting data into DB")
                    to_insert.to_sql(base_name + i, engine, if_exists='append', index=False)
                    logger.debug("end inserting data into DB")
                except Exception as e:
                    logger.error(e)
                    logger.error(
                        "error processing data for range " + str(i) + " for code " + codes.iloc[rownum]['ts_code'])


def update_bulk_balancesheet_by_ts_code_and_insert_by_year(base_name, engine, pro, codes, sharding_column, failed_count=0,
                                                     failed_tolerent=3):
    failed = []
    for code in codes['ts_code']:
        logger.debug("started processing data for " + code)
        try:
            to_insert = pro.balancesheet_vip(ts_code=code)
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
        update_bulk_balancesheet_by_ts_code_and_insert_by_year(base_name=base_name, engine=engine, pro=pro,
                                                         codes=pd.DataFrame(failed, columns=['ts_code']),
                                                         sharding_column=sharding_column,
                                                         failed_count=failed_count)
    else:
        logger.error("the below code has failed after maximum attempts. " + ','.join(failed))

logger = logging.getLogger('balancesheet_sharded')
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
# update_bulk_balancesheet_by_period_and_ts_code(base_name='balancesheet', engine=engine, pro=pro, codes=df, start_date='19950101',
# end_date=datetime.date.today().strftime("%Y%m%d"))
update_bulk_balancesheet_by_ts_code_and_insert_by_year(base_name='balancesheet', engine=engine, pro=pro, codes=df,
                                                 sharding_column='f_ann_date')
