# -*- coding: utf-8 -*-

#Load Tushare

import Utils.configuration_file_service as config_service
import tushare as ts
import time

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)


# 1. 创建表结构

from sqlalchemy import Column, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TradeCal(Base):
    """交易日历
    exchange	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
    start_date	str	N	开始日期
    end_date	str	N	结束日期
    is_open	str	N	是否交易 '0'休市 '1'交易
    """
    __tablename__ = 'trade_cal'

    exchange = Column(String(4))        # 交易所代码 SSE上交所 SZSE深交所
    cal_date = Column(String(8), primary_key=True)       # 日历日期
    pretrade_date	 = Column(String(8))     # 上一个交易日
    is_open = Column(String(1))           # 是否交易 0休市 1交易

# 2. 建立获取tushare数据函数

from sqlalchemy import create_engine
from datetime import date

today = date.today()

start = '19901219'
end = today.strftime("%Y%m%d")

def get_trade_cal(pro, retry_count=3, pause=2):
    """数据"""
    frame = pd.DataFrame()
    for exchangeid in ['SSE']:
        for _ in range(retry_count):
            try:
                df = pro.trade_cal(exchange=exchangeid, start_date=start, end_date=end)
            except:
                time.sleep(pause)
            else:
                frame = frame.append(df)
                break

    return frame

# 3. 更新数据

import pandas as pd

def truncate_update(engine, data, table_name):
    """删除mysql表所有数据，to_sql追加新数据"""
    conn = engine.connect()
    conn.execute('truncate ' + table_name)
    data.to_sql(table_name, engine, if_exists='append', index=False)


def update_trade_cal(engine, pro, retry_count, pause):
    """更新 所有数据"""
    data = get_trade_cal(pro, retry_count, pause)
    truncate_update(engine, data, 'trade_cal')

# 4. 主程序

# 创建数据库引擎
engine = create_engine(config_service.getDefaultDB())
conn = engine.connect()

# 创建mysql所有表结构
Base.metadata.create_all(engine)

# 列表
update_trade_cal(engine, pro, 3, 2)