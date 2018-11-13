#!/usr/bin/env python3

import argparse
import configparser
import logging.config
import os
import sys

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


def load_conf(conf_file):
    if conf_file is None:
        conf_file = os.path.join(bin_dir, 'config.ini')
    config = configparser.ConfigParser()
    config.read(conf_file)
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load stock market data in database')
    parser.add_argument('symbol', type=str, help='Symbol/ticker to load')
    parser.add_argument('--conf', type=str, help='Finapps configuration file')
    parser.add_argument('--historical', dest='historical', action='store_const',
                        const=bool, help='Backpopulate with historical data. Otherwise loads a daily delta')

    args = parser.parse_args()

    config = load_conf(args.conf)
    dao = finapps.stocks.dao.with_simple_pool(**dict(config.items('DATABASE')))
    svc = finapps.stocks.service.StockService(dao, config['ALPHA_VANTAGE']['api_key'])

    if args.historical:
        data = svc.historical_data_load(args.symbol)
    else:
        data = svc.incremental_data_load(args.symbol)

    logger.info('Finished')

