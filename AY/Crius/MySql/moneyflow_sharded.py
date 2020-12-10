"""
Refactored so more suitable for sharded moneyflow qfq data table
"""
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


# 1.表结构

def getTableMeta(year: int, metadata: MetaData) -> Table:
    """
    get corresponding table meta data.
    :param year: year of the data
    :return: a Table object representing the table structure
    """
    return Table(
        dbUtil.getTableName(year, "moneyflow"), metadata,
        Column("id", INT, primary_key=True),
        Column("ts_code", String(10)),  # 股票代码
        Column("trade_date", String(8)),  # 交易日期
        Column("buy_sm_vol", Float),  # 小单买入量（手）
        Column("buy_sm_amount", Float),  # 小单买入金额（万元）
        Column("sell_sm_vol", Float),  # 小单卖出量（手）
        Column("sell_sm_amount", Float),  # 小单卖出金额（万元）
        Column("buy_md_vol", Float),  # 中单买入量（手）
        Column("buy_md_amount", Float),  # 中单买入金额（万元）
        Column("sell_md_vol", Float),  # 中单卖出量（手）
        Column("sell_md_amount", Float),  # 中单卖出金额（万元）
        Column("buy_lg_vol", Float),  # 大单买入量（手）
        Column("buy_lg_amount", Float),  # 大单买入金额（万元）
        Column("sell_lg_vol", Float),  # 大单卖出量（手）
        Column("sell_lg_amount", Float),  # 大单卖出金额（万元）
        Column("buy_elg_vol", Float),  # 特大单买入量（手）
        Column("buy_elg_amount", Float),  # 特大单买入金额（万元）
        Column("sell_elg_vol", Float),  # 特大单卖出量（手）
        Column("sell_elg_amount", Float),  # 特大单卖出金额（万元）
        Column("net_mf_vol", Float),  # 净流入量（手）
        Column("net_mf_amount", Float)  # 净流入额（万元）
    )


# 2. 建立获取tushare数据函数

def get_moneyflow_code(pro, ts_code, start_date, end_date, retry_count=3, pause=2):
    """股票代码方式获取 日线行情 数据"""
    for _ in range(retry_count):
        try:
            logger.debug("start calling tushare api")
            df = pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date,
                           fields='ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,buy_md_vol,\
                           buy_md_amount,sell_md_vol,sell_md_amount,buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,\
                           buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount')
            logger.debug("end calling tushare api")

        except Exception as e:
            logger.error("error pulling data from tushare for date " + start_date)
            logger.error(e)
            time.sleep(pause)
        else:
            return df


def get_moneyflow_date(pro, date, retry_count=3, pause=2):
    """日期方式获取 日线行情 数据"""
    for _ in range(retry_count):
        try:
            logger.debug("starting calling tushare api")
            df = pro.moneyflow(trade_date=date,
                           fields='ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,buy_md_vol,\
                           buy_md_amount,sell_md_vol,sell_md_amount,buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,\
                           buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount')
            logger.debug("end calling tushare api")
        except Exception as e:
            logger.error("error pulling data from tushare for date " + date)
            logger.error(e)
            time.sleep(pause)
        else:
            return df


# 3. 更新数据


def get_ts_code(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code from stock_basic', engine)


def get_trade_cal(pro, start_date, end_date):
    return pro.trade_cal(start_date=start_date, end_date=end_date, is_open='1')["cal_date"]


def update_bulk_moneyflow_using_code_by_year(engine, pro, codes, start_date, end_date, retry_count, pause):
    """
    股票代码方式更新 日线行情

    """
    start_year = int(start_date[0:4])
    end_year = int(end_date[0:4])
    for i in range(start_year, end_year):
        logger.info("starting processing year " + str(i))
        if (i == start_year):
            temp_start_date = start_date
            temp_end_date = str(i) + "1231"
        elif (i == end_year):
            temp_start_date = str(i) + "0101"
            temp_end_date = end_date
        else:
            temp_start_date = str(i) + "0101"
            temp_end_date = str(i) + "1231"
        for value in codes['ts_code']:
            logger.debug("processing " + value + " for date " + temp_start_date + "-" + temp_end_date)
            df = get_moneyflow_code(pro, value, temp_start_date, temp_end_date, retry_count, pause)
            try:
                logger.debug("start inserting data into DB")
                df.to_sql(dbUtil.getTableName(i, "moneyflow"), engine, if_exists='append', index=False)
                logger.debug("end inserting data into DB")
            except IntegrityError as err:
                logger.error("error processing data for year" + str(i) + " stock code " + value)
                logger.error(err)


def update_moneyflow_date(engine, pro, date, retry_count, pause):
    """日期方式更新 日线行情"""
    df = get_moneyflow_date(pro, date, retry_count, pause)
    df.to_sql(dbUtil.getTableName(int(date[0:4]), "moneyflow"), engine, if_exists='append', index=False)


def update_bulk_moneyflow_by_day(engine, pro, start_date, end_date):
    trade_cal = get_trade_cal(pro, start_date, end_date)
    for a_day in trade_cal:
        logger.debug("started processing data for date " + a_day)
        df = get_moneyflow_date(pro=pro, date=a_day)
        try:
            logger.debug("start inserting data into DB")
            df.to_sql(dbUtil.getTableName(int(a_day[0:4]), "moneyflow"), engine, if_exists='append', index=False)
            logger.debug("end inserting data into DB")
        except IntegrityError as err:
            logger.error("error processing data for date" + str(a_day) + " as data for that day already exists")
            logger.error(err)
        except Exception as e:
            logger.error("error processing data for date" + str(a_day))
            logger.error(e)

# 4. 主程序

logger = logging.getLogger('moneyflow_sharded')
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

# codes = get_ts_code(engine)
# update_bulk_moneyflow_using_code_by_year(engine, pro, codes, '19901219', datetime.date.today().strftime("%Y%m%d"), 3, 1)

update_bulk_moneyflow_by_day(engine=engine, pro=pro, start_date='19901219', end_date=datetime.date.today().strftime("%Y%m%d"))
