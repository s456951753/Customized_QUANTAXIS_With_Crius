import pandas as pd
from QUANTAXIS.QAUtil.QASetting import DATABASE
import datetime


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
    return calendar[(calendar['date'] >= int(start_date)) & calendar['date'] <= int(end_date)]


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
