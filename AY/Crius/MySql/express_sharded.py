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
        dbUtil.getTableName(year, "express"), metadata,
        Column("ts_code", String(10)),  # 股票代码
        Column("ann_date", String(8)),
        Column("end_date", String(8)),
        Column("revenue", Float),  # 营业收入(元)
        Column("operate_profit", Float),  # 营业利润(元)
        Column("total_profit", Float),  # 利润总额(元)
        Column("n_income", Float),  # 净利润(元)
        Column("total_assets", Float),  # 总资产(元)
        Column("total_hldr_eqy_exc_min_int", Float),  # 股东权益合计(不含少数股东权益)(元)
        Column("diluted_eps", Float),  # 每股收益(摊薄)(元)
        Column("diluted_roe", Float),  # 净资产收益率(摊薄)(%)
        Column("yoy_net_profit", Float),  # 去年同期修正后净利润
        Column("bps", Float),  # 每股净资产
        Column("yoy_sales", Float),  # 同比增长率:营业收入
        Column("yoy_op", Float),  # 同比增长率:营业利润
        Column("yoy_tp", Float),  # 同比增长率:利润总额
        Column("yoy_dedu_np", Float),  # 同比增长率:归属母公司股东的净利润
        Column("yoy_eps", Float),  # 同比增长率:基本每股收益
        Column("yoy_roe", Float),  # 同比增减:加权平均净资产收益率
        Column("growth_assets", Float),  # 比年初增长率:总资产
        Column("yoy_equity", Float),  # 比年初增长率:归属母公司的股东权益
        Column("growth_bps", Float),  # 比年初增长率:归属于母公司股东的每股净资产
        Column("or_last_year", Float),  # 去年同期营业收入
        Column("op_last_year", Float),  # 去年同期营业利润
        Column("tp_last_year", Float),  # 去年同期利润总额
        Column("np_last_year", Float),  # 去年同期净利润
        Column("eps_last_year", Float),  # 去年同期每股收益
        Column("open_net_assets", Float),  # 期初净资产
        Column("open_bps", Float)  # 期初每股净资产
    )

def get_ts_code(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code from stock_basic', engine)

def get_ts_code_and_list_date(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code,list_date from stock_basic', engine)


def update_bulk_express_by_period_and_ts_code(base_name, engine, pro, codes, start_date, end_date, retry_count=3,
                                             pause=2):
    coverage = dbUtil.getTableRange(base_name="", start_date=start_date, end_date=end_date)
    for i in coverage:
        for rownum in range(0, len(codes)):
            logger.debug("started processing data for " + codes.iloc[rownum]['ts_code'] + " for period " + i)
            if (int(codes.iloc[rownum]['list_date'][0:4]) <= int(i[1:5]) or int(
                    codes.iloc[rownum]['list_date'][0:4]) <= int(i[6:10])):
                try:
                    to_insert = pro.express_vip(ts_code=codes.iloc[rownum]['ts_code'], start_date=i[1:5] + '0101',
                                               end_date=i[6:10] + '1231', fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps')
                    logger.debug("start inserting data into DB")
                    to_insert.to_sql(base_name + i, engine, if_exists='append', index=False)
                    logger.debug("end inserting data into DB")
                except Exception as e:
                    logger.error(e)
                    logger.error(
                        "error processing data for range " + str(i) + " for code " + codes.iloc[rownum]['ts_code'])


def update_bulk_express_by_ts_code_and_insert_by_year(base_name, engine, pro, codes, sharding_column, failed_count=0,
                                                     failed_tolerent=3):
    failed = []
    for code in codes['ts_code']:
        logger.debug("started processing data for " + code)
        try:
            to_insert = pro.express_vip(ts_code=code, fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps')
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
        update_bulk_express_by_ts_code_and_insert_by_year(base_name=base_name, engine=engine, pro=pro,
                                                         codes=pd.DataFrame(failed, columns=['ts_code']),
                                                         sharding_column=sharding_column,
                                                         failed_count=failed_count)
    else:
        logger.error("the below code has failed after maximum attempts. " + ','.join(failed))

logger = logging.getLogger('express_sharded')
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
# update_bulk_express_by_period_and_ts_code(base_name='express', engine=engine, pro=pro, codes=df, start_date='19950101',
# end_date=datetime.date.today().strftime("%Y%m%d"))
update_bulk_express_by_ts_code_and_insert_by_year(base_name='express', engine=engine, pro=pro, codes=df,
                                                 sharding_column='ann_date')
