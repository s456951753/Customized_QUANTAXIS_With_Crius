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
        dbUtil.getTableName(year, "income"), metadata,
        Column("id", INT, primary_key=True),
        Column("ts_code", String(10)),  # 股票代码
        Column("ann_date", String(8)),
        Column("f_ann_date", String(8)),
        Column("end_date", String(8)),
        Column("report_type", String(3)),
        Column("comp_type", String(3)),
        Column("basic_eps", Float),  # 基本每股收益
        Column("diluted_eps", Float),  # 稀释每股收益
        Column("total_revenue", Float),  # 营业总收入
        Column("revenue", Float),  # 营业收入
        Column("int_income", Float),  # 利息收入
        Column("prem_earned", Float),  # 已赚保费
        Column("comm_income", Float),  # 手续费及佣金收入
        Column("n_commis_income", Float),  # 手续费及佣金净收入
        Column("n_oth_income", Float),  # 其他经营净收益
        Column("n_oth_b_income", Float),  # 加:其他业务净收益
        Column("prem_income", Float),  # 保险业务收入
        Column("out_prem", Float),  # 减:分出保费
        Column("une_prem_reser", Float),  # 提取未到期责任准备金
        Column("reins_income", Float),  # 其中:分保费收入
        Column("n_sec_tb_income", Float),  # 代理买卖证券业务净收入
        Column("n_sec_uw_income", Float),  # 证券承销业务净收入
        Column("n_asset_mg_income", Float),  # 受托客户资产管理业务净收入
        Column("oth_b_income", Float),  # 其他业务收入
        Column("fv_value_chg_gain", Float),  # 加:公允价值变动净收益
        Column("invest_income", Float),  # 加:投资净收益
        Column("ass_invest_income", Float),  # 其中:对联营企业和合营企业的投资收益
        Column("forex_gain", Float),  # 加:汇兑净收益
        Column("total_cogs", Float),  # 营业总成本
        Column("oper_cost", Float),  # 减:营业成本
        Column("int_exp", Float),  # 减:利息支出
        Column("comm_exp", Float),  # 减:手续费及佣金支出
        Column("biz_tax_surchg", Float),  # 减:营业税金及附加
        Column("sell_exp", Float),  # 减:销售费用
        Column("admin_exp", Float),  # 减:管理费用
        Column("fin_exp", Float),  # 减:财务费用
        Column("assets_impair_loss", Float),  # 减:资产减值损失
        Column("prem_refund", Float),  # 退保金
        Column("compens_payout", Float),  # 赔付总支出
        Column("reser_insur_liab", Float),  # 提取保险责任准备金
        Column("div_payt", Float),  # 保户红利支出
        Column("reins_exp", Float),  # 分保费用
        Column("oper_exp", Float),  # 营业支出
        Column("compens_payout_refu", Float),  # 减:摊回赔付支出
        Column("insur_reser_refu", Float),  # 减:摊回保险责任准备金
        Column("reins_cost_refund", Float),  # 减:摊回分保费用
        Column("other_bus_cost", Float),  # 其他业务成本
        Column("operate_profit", Float),  # 营业利润
        Column("non_oper_income", Float),  # 加:营业外收入
        Column("non_oper_exp", Float),  # 减:营业外支出
        Column("nca_disploss", Float),  # 其中:减:非流动资产处置净损失
        Column("total_profit", Float),  # 利润总额
        Column("income_tax", Float),  # 所得税费用
        Column("n_income", Float),  # 净利润(含少数股东损益)
        Column("n_income_attr_p", Float),  # 净利润(不含少数股东损益)
        Column("minority_gain", Float),  # 少数股东损益
        Column("oth_compr_income", Float),  # 其他综合收益
        Column("t_compr_income", Float),  # 综合收益总额
        Column("compr_inc_attr_p", Float),  # 归属于母公司(或股东)的综合收益总额
        Column("compr_inc_attr_m_s", Float),  # 归属于少数股东的综合收益总额
        Column("ebit", Float),  # 息税前利润
        Column("ebitda", Float),  # 息税折旧摊销前利润
        Column("insurance_exp", Float),  # 保险业务支出
        Column("undist_profit", Float),  # 年初未分配利润
        Column("distable_profit", Float),  # 可分配利润
        Column("update_flag", Float)  # 更新标识，0未修改1更正过
    )

def get_ts_code(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code from stock_basic', engine)

def get_ts_code_and_list_date(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code,list_date from stock_basic', engine)


def update_bulk_income_by_period_and_ts_code(base_name, engine, pro, codes, start_date, end_date, retry_count=3,
                                             pause=2):
    coverage = dbUtil.getTableRange(base_name="", start_date=start_date, end_date=end_date)
    for i in coverage:
        for rownum in range(0, len(codes)):
            logger.debug("started processing data for " + codes.iloc[rownum]['ts_code'] + " for period " + i)
            if (int(codes.iloc[rownum]['list_date'][0:4]) <= int(i[1:5]) or int(
                    codes.iloc[rownum]['list_date'][0:4]) <= int(i[6:10])):
                try:
                    to_insert = pro.income_vip(ts_code=codes.iloc[rownum]['ts_code'], start_date=i[1:5] + '0101',
                                               end_date=i[6:10] + '1231')
                    logger.debug("start inserting data into DB")
                    to_insert.to_sql(base_name + i, engine, if_exists='append', index=False)
                    logger.debug("end inserting data into DB")
                except Exception as e:
                    logger.error(e)
                    logger.error(
                        "error processing data for range " + str(i) + " for code " + codes.iloc[rownum]['ts_code'])


def update_bulk_income_by_ts_code_and_insert_by_year(base_name, engine, pro, codes, sharding_column, failed_count=0,
                                                     failed_tolerent=3):
    failed = []
    for code in codes['ts_code']:
        logger.debug("started processing data for " + code)
        try:
            to_insert = pro.income_vip(ts_code=code)
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
    if (failed_count < failed_tolerent and len(failed)>0):
        logger.warning("retrying now.")
        failed_count = failed_count + 1
        update_bulk_income_by_ts_code_and_insert_by_year(base_name=base_name, engine=engine, pro=pro,
                                                         codes=pd.DataFrame(failed, columns=['ts_code']),
                                                         sharding_column=sharding_column,
                                                         failed_count=failed_count)
    else:
        logger.error("the below code has failed after maximum attempts. " + ','.join(failed))

logger = logging.getLogger('income_sharded')
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
# update_bulk_income_by_period_and_ts_code(base_name='income', engine=engine, pro=pro, codes=df, start_date='19950101',
# end_date=datetime.date.today().strftime("%Y%m%d"))

update_bulk_income_by_ts_code_and_insert_by_year(base_name='income', engine=engine, pro=pro, codes=df,
                                                 sharding_column='f_ann_date')

# For failed codes
# array = ['000001.SZ','000002.SZ']
# codes = pd.DataFrame(array, columns=['ts_code']),

# update_bulk_income_by_ts_code_and_insert_by_year(base_name='income', engine=engine, pro=pro, codes=codes,
#                                                 sharding_column='f_ann_date')
