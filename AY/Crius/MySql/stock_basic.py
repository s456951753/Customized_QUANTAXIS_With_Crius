# -*- coding: utf-8 -*-

#Load Tushare

import AY.Crius.Utils.configuration_file_service as config_service
import tushare as ts
import time

token = "3c311a0c0eb056bfe6c27a161e6cab275649b74245cfd5679a75dca9"

#token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
#                                   property_name=config_service.TS_TOKEN_NAME)

pro = ts.pro_api(token)

# 1.创建表结构
from sqlalchemy import Column, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
'''
class StockBasic(Base):
    """股票列表
    is_hs	    str	N	是否沪深港通标的，N否 H沪股通 S深股通
    list_status	str	N	上市状态： L上市 D退市 P暂停上市
    exchange	str	N	交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
    """
    __tablename__ = 'stock_basic'

    ts_code = Column(String(10), primary_key=True)  # TS代码
    symbol = Column(String(10))         # 股票代码
    name = Column(String(10))           # 股票名称
    area = Column(String(4))            # 所在地域
    industry = Column(String(4))        # 所属行业
    fullname = Column(String(30))       # 股票全称
    enname = Column(String(100))        # 英文全称
    market = Column(String(3))          # 市场类型 （主板/中小板/创业板）
    exchange = Column(String(4))        # 交易所代码
    curr_type = Column(String(3))       # 交易货币
    list_status = Column(String(1))     # 上市状态： L上市 D退市 P暂停上市
    list_date = Column(String(8))       # 上市日期
    delist_date = Column(String(8))     # 退市日期
    is_hs = Column(String(1))           # 是否沪深港通标的，N否 H沪股通 S深股通
'''
# 2. 建立获取tushare数据函数

from sqlalchemy import create_engine

def get_stock_basic(pro, retry_count=3, pause=2):
    """数据"""
    frame = pd.DataFrame()
    for status in ['L', 'D', 'P']:
        for _ in range(retry_count):
            try:
                df = pro.stock_basic(exchange='', list_status=status,
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
#engine = create_engine(config_service.getDefaultDB())
#conn = engine.connect()

# 创建mysql所有表结构
#Base.metadata.create_all(engine)

# 列表
#update_stock_basic(engine, pro, 3, 2)
