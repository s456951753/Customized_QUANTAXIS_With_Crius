# -*- coding: utf-8 -*-

#Load Tushare

import Utils.configuration_file_service as config_service
import tushare as ts
import time

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)

# 1.创建表结构
from sqlalchemy import Column, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ProBar(Base):
    """A股复权行情
    不复权	无	空或None
    前复权	当日收盘价 × 当日复权因子 / 最新复权因子	qfq
    后复权	当日收盘价 × 当日复权因子	hfq
    """
    __tablename__ = 'pro_bar'

    ts_code = Column(String(10), primary_key=True)  # TS代码
    adj = Column(String(4))         # 复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
    freq = Column(String(1))           # 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D, W=week, M=Month
    start_date = Column(String(8))       # 开始日期 (格式：YYYYMMDD)
    end_date = Column(String(8))     # 结束日期 (格式：YYYYMMDD)

# 2. 建立获取tushare数据函数

from sqlalchemy import create_engine

def get_pro_bar(pro, retry_count=3, pause=2):
    """数据"""
    frame = pd.DataFrame()
    for adjstatus in ['qfq']:
        for _ in range(retry_count):
            try:
                df = ts.pro_bar(exchange='', list_status=status,
                                     fields='ts_code,symbol,name,area,industry,fullname,enname,market, \
                                    exchange,curr_type,list_status,list_date,delist_date,is_hs')
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


def update_stock_basic(engine, pro, retry_count, pause):
    """更新 所有数据"""
    data = get_stock_basic(pro, retry_count, pause)
    truncate_update(engine, data, 'stock_basic')

# 4. 主程序

# 创建数据库引擎
engine = create_engine(config_service.getDefaultDB())
conn = engine.connect()

# 创建mysql所有表结构
Base.metadata.create_all(engine)

# 列表
update_stock_basic(engine, pro, 3, 2)
