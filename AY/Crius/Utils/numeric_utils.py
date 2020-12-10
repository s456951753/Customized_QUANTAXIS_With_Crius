# DONE: #请define 一个新功能来转化Tushare 和 Rqalpha 的股票代码。 一个功能是 Tushare 转 Rqalpha 另一个是 Rqalpha 转 Tushare
# DONE:Tushare 的股票代码格式 举例： 000690.SZ （深市）or 603328.SH （沪市）
# DONE:Rqalpha 的股票代码格式 举例： 000690.XSHE （深市）or 603328.XSHG （沪市）

import re
import dateutil
import datetime

from pandas import DataFrame


def get_converted_stock_code(symbol):
    """
    Based on the symbol given, convert symbol from rqalpha to tushare or vise versa. Will check if the symbol meets format
    criteria, i.e. ######.SZ/XSHE/SH/XSHG and throw SymbolInvalidException if didn't meet
    :raises SymbolInvalidException if the symbol name does not meet the naming format
    """
    validate(symbol)
    symbol = str(symbol)
    new_symbol = symbol.split('.')
    dic = {'SZ': 'XSHE',
           'SH': 'XSHG',
           'XSHE': 'SZ',
           'XSHG': 'SH'}
    return new_symbol[0] + '.' + dic.get(new_symbol[1])


def get_closest_half_year(snapshot_date=datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")):
    """
    find the closest end day of June/December, depends on which one is closer to the given date.
    :raise ValueError if format is invalid
    :param snapshot_date:a date string in format %Y%m%d. If left empty, today's date will be used.
    :return: a string object %Y%m%d, representing the closest end day of June/December, depends on which one is closer to the given date
    """
    try:
        datetime.datetime.strptime(snapshot_date, '%Y%m%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYYMMDD")
    new_date = datetime.date.fromisoformat(snapshot_date[0:4] + "-" + snapshot_date[4:6] + "-" + snapshot_date[6:])
    new_year = new_date.year
    new_month = new_date.month
    new_day = new_date.day
    if (new_month < 6):
        new_month = 12
        new_day = 31
        new_year = new_year - 1
    elif (new_month > 6 and new_month < 12):
        new_month = 6
        new_day = 30
    elif (new_month == 6):
        if (new_day != 30):
            new_month = 12
            new_day = 31
            new_year = new_year - 1
    elif (new_month == 12):
        if (new_day != 31):
            new_month = 6
            new_day = 30

    return datetime.datetime.strftime(datetime.date(new_year, new_month, new_day), "%Y%m%d")


def get_list_of_converted_stock_code(symbols):
    """
    provides list support for symbol conversion.
    :param symbols: a list(array) of symbols
    :return: a list of symbols
    :raises: SymbolInvalidException as defined in get_converted_stock_code
    """
    returnlist = []
    for symbol in symbols:
        returnlist.append(get_converted_stock_code(symbol))
    return returnlist

def validate(symbol):
    a = re.compile("\d{6}\.SZ|SH|XSHE|XSHG")
    if (a.search(symbol) == None):
        raise SymbolInvalidException(symbol)


class SymbolInvalidException(Exception):
    def __init__(self, symbol):
        self.symbol = symbol
        self.message = symbol + " is not a valid symbol"


def get_last_x_trading_day(list_of_trading_date: DataFrame, x_day_ago=3):
    list_of_trading_date = list_of_trading_date[list_of_trading_date.is_open == 1]
    return list_of_trading_date['cal_date'].tolist()[-1 * x_day_ago]


def get_quarter_start_end_date(quarter: int):
    start_of_quarter = ['0101', '0401', '0701', '1001']
    end_of_quarter = ['0331', '0630', '0930', '1231']

    return {start_of_quarter[quarter], end_of_quarter[quarter]}
