import logging
import sys

import rqalpha.portfolio
import rqalpha.model.order
import time
from rqalpha.const import ORDER_STATUS


def get_info_for_order(order: rqalpha.model.order, portfolio: rqalpha.portfolio, logger, logging_level="info"):
    breaker = 0
    while (not order.is_final() and breaker < 5):
        logger.debug("order is not finalized. Sleeping for 1 sec")
        time.sleep(1)
        breaker = breaker + 1
    if (breaker == 5):
        logger.error("order not finalized in 5 seconds. your script might have stuck")
    logger.info(
        order.datetime.strftime("%Y%m%d") + " order " + str(
            order.order_id) + " exercised. " + order.order_book_id + " fulfilled " + str(
            order.filled_quantity) + " at " + str(order.avg_price))
    logger.info(
        "current cash level:" + str(portfolio.cash) + ". " + "total value is " + str(
            portfolio.total_value) + ". total return by today is " + str(portfolio.total_returns))
    if (logging_level == "info"):
        pass
    logger.debug("detailed info for " + str(order.order_id) + ": ")
    if (order.price == 0):
        logger.debug("placed: " + str(order.quantity) + " of " + str(order.order_book_id))
    else:
        logger.debug("placed: " + str(order.quantity) + " at " + str(order.price))
    logger.debug("fulfilled: " + str(order.filled_quantity) + " at " + str(order.avg_price))
    logger.debug("current positions:")
    logger.debug(portfolio.positions)


def get_logger(base_name, file_name=None):
    """
    get a logger that write logs to both stdout and a file. Default logging level is info so remember to
    :param base_name:
    :param file_name:
    :param logging_level:
    :return:
    """
    if (file_name is None):
        file_name = base_name
    logger = logging.getLogger(base_name)
    logger.setLevel(logging.INFO)

    # create console handler & file handler
    ch = logging.StreamHandler()
    ch.setStream(sys.stdout)
    fi = logging.FileHandler(filename="E:\PycharmProjects\Crius\\" + file_name + ".log")

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to channels
    ch.setFormatter(formatter)
    fi.setFormatter(formatter)
    # add channels to logger
    logger.addHandler(ch)
    logger.addHandler(fi)
    return logger
