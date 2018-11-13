#!/usr/bin/env python3

import argparse
import configparser
import datetime
import logging.config
import os
import sys

import pandas as pd

# logging has to be configured before our app imports, to ensure the global configuration is applied
# before any loggers are constructed...
bin_dir = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/..')
log_conf = os.path.join(bin_dir, 'logging.ini')
logging.config.fileConfig(log_conf)
logger = logging.getLogger(os.path.basename(__file__))

# add finapps to path..
project = os.path.realpath(bin_dir + '/..')
sys.path.append(project)

import finapps.stocks.dao
import finapps.stocks.service


def load_conf(conf_file: str):
    if conf_file is None:
        conf_file = os.path.join(bin_dir, 'config.ini')
    config = configparser.ConfigParser()
    config.read(conf_file)
    return config


def valid_date(s: str):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def print_digest(digest: pd.DataFrame):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    print(digest.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description='Create performance digest for a stock')
    parser.add_argument('symbol', type=str, help='Symbol/ticker to digest')
    parser.add_argument('date', type=valid_date, help='Date to create digest for')
    parser.add_argument('--conf', type=str, help='Finapps configuration file')

    args = parser.parse_args()

    config = load_conf(args.conf)
    dao = finapps.stocks.dao.with_simple_pool(**dict(config.items('DATABASE')))
    svc = finapps.stocks.service.StockService(dao, config['ALPHA_VANTAGE']['api_key'])

    min_date, max_date = svc.min_max_dates(args.symbol)
    logging.info(f"[{args.symbol}] min persisted date: {min_date.strftime('%Y-%m-%d')}")
    logging.info(f"[{args.symbol}] max persisted date: {max_date.strftime('%Y-%m-%d')}")

    digest = svc.digest(args.symbol, args.date)
    print_digest(digest)
    logger.info('Finished')


if __name__ == '__main__':
    main()

