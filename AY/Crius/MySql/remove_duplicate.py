"""
Refactored so more suitable for sharded daily qfq data table
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

engine = create_engine(config_service.getDefaultDB())
conn = engine.connect()

logger = logging.getLogger('daily_sharded')
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

import mysql.connector as mysql

mydb = mysql.connect(
        host=config_service.getDefaultDB_cursor_host(),
        user=config_service.getDefaultDB_cursor_user(),
        passwd=config_service.getDefaultDB_cursor_passwd(),
       database=config_service.getDefaultDB_cursor_database(),)

def deleteDuplicates(db_table):
    stop = False
    while (not stop):
        mycursor = mydb.cursor()
        get_dup_query = "select id from (select min(x.id) as id,x.ts_code,x.ann_date from (SELECT id," + db_table + ".ts_code as ts_code," + db_table + ".ann_date as ann_date FROM " + db_table + " INNER JOIN (SELECT ann_date,ts_code FROM " + db_table + " GROUP BY ann_date,ts_code HAVING COUNT(id) > 1) dup ON " + db_table + ".ann_date = dup.ann_date and " + db_table + ".ts_code = dup.ts_code)x GROUP BY x.ann_date,x.ts_code)y;"
        mycursor.execute(get_dup_query)
        databaseIds = mycursor.fetchall()
        print("Total rows are:  ", len(databaseIds))
        if len(databaseIds) == 0:
            stop = True
        for id in databaseIds:
            id_value = id[0]
            delete_query = "DELETE FROM " + db_table + " WHERE id = '{0}';".format(id_value)
            # print("id : ",id[0])
            mycursor.execute(delete_query)
            print(delete_query)
        mycursor.close()
        mydb.commit()

    print("Process complete!!!")


# 主程序

db_table_list = ["fina_indicator_1990_1994", "fina_indicator_1995_1999", "fina_indicator_2000_2004", "fina_indicator_2005_2009", "fina_indicator_2010_2014", "fina_indicator_2015_2019", "fina_indicator_2020_2024"]

for db_table in db_table_list:
  deleteDuplicates(db_table)