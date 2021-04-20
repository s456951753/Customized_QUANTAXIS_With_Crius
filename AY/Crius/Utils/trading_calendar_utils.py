import pandas as pd
from QUANTAXIS.QAUtil.QASetting import DATABASE
import datetime

START_DATE_OF_SHSX_STOCK = '19930101'


def get_trading_days_between(start_date=None, end_date=None) -> pd.DataFrame:
    '''
    return all dates before end_date if start_date is null, inclusive of end_date
    return all dates after start_date if end_date is null, inclusive of start_date
    otherwise give all dates between the two given date
    :param start_date:
    :param end_date:
    :return:
    '''
    if (start_date is None and end_date is None):
        raise ValueError("Start and end date must not be None at the same time")
    calendar = get_trading_calendar()
    '''
    return all dates before end_date if start_date is null, inclusive of end_date
    '''
    if (start_date is None):
        return calendar[calendar['date'] <= int(end_date)]

    '''
    return all dates after start_date if end_date is null, inclusive of start_date
    '''
    if (end_date is None):
        return calendar[calendar['date'] >= int(start_date)]

    #    otherwise give all dates between the two given date
    return calendar[(int(start_date) <= calendar['date']) & (calendar['date'] <= int(end_date))]


def get_last_x_trading_day_from_mongodb(x_day_ago=1):
    '''
    return the date as int specified by the days ago parameter. 1 will be yesterday and 2 will be the day before yesterday
    :param x_day_ago:
    :return:
    '''
    list_of_trading_date = get_trading_calendar()
    return list_of_trading_date['date'].tolist()[-1 * x_day_ago]


def get_trading_calendar(engine=DATABASE) -> pd.DataFrame:
    today = int(datetime.date.today().strftime(format="%Y%m%d"))

    cursor = DATABASE.trade_date.find(projection={"_id": 0, "date": 1, "exchangeName": 1})
    list_of_cursor = list(cursor)

    df = pd.DataFrame(list_of_cursor)
    df['date'] = df['date'].astype(int)
    df = df[df.date < today]
    return df


def get_today_as_str():
    return datetime.date.today().strftime(format="%Y%m%d")


def get_yesterday_as_str():
    yesterday = datetime.datetime.now() - datetime.timedelta(1)
    return datetime.datetime.strftime(yesterday, format="%Y%m%d")


def get_days_between(start_date=None, end_date=None):
    '''
    logic:
        if start_date and end_date are not provided, return yesterday
        if start_date and end_date equals, return the day
        if start_date not provided, return from 19930101 to end_date, inclusive of end_date
        if end_date not provided, return from start_date to today, inclusive of both sides
    Args:
        start_date:
        end_date:

    Returns:
        single-column dataframe containing the days, with column name "date"
    '''
    __list = []

    if (start_date is None and end_date is None):
        df= pd.DataFrame(__list.append(get_yesterday_as_str()), columns=['date'])
        return df
    if start_date is None:
        start_date = START_DATE_OF_SHSX_STOCK
    if end_date is None:
        end_date = get_today_as_str()
    if start_date == end_date:
        df = pd.DataFrame(__list.append(str(start_date)), columns=['date'])
        return df
    start_as_datetime = datetime.datetime.strptime(start_date, '%Y%m%d')
    end_as_datetime = datetime.datetime.strptime(end_date, '%Y%m%d')
    span = end_as_datetime - start_as_datetime

    for n in range(span.days + 1):
        day = start_as_datetime + datetime.timedelta(days=n)
        __list.append(day.strftime(format="%Y%m%d"))

    return pd.DataFrame(__list, columns=['date'])
