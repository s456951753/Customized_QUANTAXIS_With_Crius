import datetime
import math

import Utils.configuration_file_service as config_service
import logging

VERY_BEGINNING_YEAR = 1990


def getTableName(year: int, base_name) -> str:
    """
    Calculate table name out based on the give date and base api name. The table name will be in the below format:
    basename_startyear_endyear
    Specifically, the difference between start and end year will be 4 so each table contains 5 years of data
    - ranges will be like 1990 to 1994, 1995 to 1999

    :param year: year of the data you want to insert
    :param base_name: name of the api
    :return: basename_startyear_endyear. If the given year is not in the range supported (1990 to current year),
    basename_9995_10000 wil be returned. Hopefully we are not using this piece of shit by then.
    """
    tableName = None
    startEndYears = getYears()
    for startYear in startEndYears.keys():
        if (year >= startYear and year <= startEndYears.get(startYear)):
            tableName = base_name + '_' + str(startYear) + '_' + str(startEndYears.get(startYear))
            break
    if (tableName == None):
        logging.warning(
            "The provided year argument " + str(
                year) + " is outside of the storage time interval. Please double check. We currently support between " + str(
                VERY_BEGINNING_YEAR) + " and " + str(datetime.date.today().year))
        return base_name + '_' + str(9995) + '_' + str(10000)
    return tableName


def getYears() -> {}:
    """
    Return a list of calculated start and end years.
    :return: a list of calculated start and end years.
    """
    startEndYears = {}
    # year = config_service.getProperty(config_service.DATA_CONFIG_SECTION_NAME,
    #                                 config_service.DATA_CONFIG_YEAR_GRANULARITY_NAME)
    currentYear = datetime.date.today().year

    thisYear = VERY_BEGINNING_YEAR
    while (thisYear <= currentYear):
        # startEndYears.append(str(thisYear) + '_' + str(thisYear+5))
        startEndYears[thisYear] = thisYear + 4
        thisYear = thisYear + 5
    if (thisYear > currentYear):
        # startEndYears.append(str(thisYear-5) + '_' + str(thisYear))
        startEndYears[thisYear - 5] = thisYear - 1
    return startEndYears


def getTableRange(base_name: str, start_date: str, end_date: str):
    """
    Get a collection of table names based on the given date
    :param base_name:
    :param start_date:
    :param end_date:
    :return: an array of table names, or None if date is not valid
    """
    assert int(start_date) <= int(end_date)
    assert VERY_BEGINNING_YEAR <= int(start_date[0:4]) <= datetime.date.today().year
    assert VERY_BEGINNING_YEAR <= int(end_date[0:4]) <= datetime.date.today().year

    start_year = int(start_date[0:4])
    end_year = int(end_date[0:4])
    years = getYears()

    names = []
    end_year_pos = []
    # as long as start or end year of a table is covered, we know
    for i in years.keys():
        if (start_year <= years.get(i) <= end_year or start_year <= i <= end_year):
            names.append(base_name + '_' + str(i) + '_' + str(years.get(i)))
    # if nothing in names, gap between input & output < 4 years, not enough to cover one table
    if len(names) == 0:
        for i in years.keys():
            if (i <= start_year and years.get(i) >= end_year):
                names.append(base_name + '_' + str(i) + '_' + str(years.get(i)))
    return names
