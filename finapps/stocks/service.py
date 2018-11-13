import datetime
import logging
import urllib.parse

import ballpark
import pandas as pd
import requests
import json

from finapps.stocks.dao import MarketDataDAO

LOGGER = logging.getLogger(__name__)


class DigestError(Exception):
    pass


class StockService:
    """
    Encapsulates business logic for the Stocks application
    """
    def __init__(self, dao: MarketDataDAO, alpha_vantage_api_key: str):
        self.dao = dao
        self.api_key = alpha_vantage_api_key

    def historical_data_load(self, symbol: str):
        """
        Perform historical data load for the given symbol
        """
        LOGGER.info(f'[{symbol}] downloading historical data')
        df = StockService.alpha_vantage_download(symbol, self.api_key, historical=True)
        LOGGER.info(f'[{symbol}] persisting {len(df)} records')
        self.dao.insert(df)
        LOGGER.info(f'[{symbol}] done')

    def incremental_data_load(self, symbol: str):
        """
        Perform incremental data load for the given symbol
        """
        LOGGER.info(f'[{symbol}] downloading incremental data')
        df = StockService.alpha_vantage_download(symbol, self.api_key, historical=False)
        LOGGER.info(f'[{symbol}] persisting {len(df)} records')
        self.dao.insert(df)
        LOGGER.info(f'[{symbol}] done')

    def min_max_dates(self, symbol: str):
        """
        Obtain the minimum and maximum persisted market dates for given symbol
        """
        return self.dao.min_max_dates(symbol)

    def maybe_get_data(self, symbol: str, as_of_date: datetime.date, look_back=5):
        """
        Attempt to return persisted market data for the given symbol and date. If no data is
        available for the given date, this function will look-back a maximum number of days to find data.
        """
        lo_date = as_of_date - datetime.timedelta(days=look_back)
        df = self.dao.select(symbol, lo_date, as_of_date)
        if df.empty:
            LOGGER.warning(f"[{symbol}] has no data for {as_of_date.strftime('%Y-%m-%d')}")
            return df
        else:
            data = df.sort_values(by='MKT_DATE', ascending=False).head(1)
            if data['MKT_DATE'].iloc[0].to_pydatetime() != as_of_date:
                LOGGER.info(f"[{symbol}] has no data for {as_of_date.strftime('%Y-%m-%d')}, "
                            f"using closest available date "
                            f"{data['MKT_DATE'].iloc[0].to_pydatetime().strftime('%Y-%m-%d')}")
            return data

    def digest(self, symbol: str, as_of_date: datetime.date) -> pd.DataFrame:
        """
        Create a performance digest for the given symbol on the given date. The digest compares
        the performance on the given date to 1d,1m,3m,6m,1y,3yr prior.
        """
        def digest_str(value, compare_value, ballpark_value=False):
            pct = int((compare_value - value) / value * 100)
            if pct > 0:
                indicator = '↑'
            elif pct < 0:
                indicator = '↓'
            else:
                indicator = '→'

            if ballpark_value:
                value = ballpark.business(value)
            else:
                value = f'{value:,.0f}'

            return f'{value} ({indicator} {pct}%)'

        # strict - must have data for the digest mkt data. known as the zero'th date.
        as_of_data = self.maybe_get_data(symbol, as_of_date, look_back=0)
        if as_of_data.empty:
            raise DigestError(f"Missing market data for as-of-date {as_of_date.strftime('%Y-%m-%d')}")

        config = {
            '1d': as_of_date - datetime.timedelta(days=1),
            '1m': as_of_date - datetime.timedelta(days=30),
            '3m': as_of_date - datetime.timedelta(days=90),
            '6m': as_of_date - datetime.timedelta(days=180),
            '1y': as_of_date - datetime.timedelta(days=365),
            '3y': as_of_date - datetime.timedelta(days=365 * 3)
        }
        data_by_label = {label: self.maybe_get_data(symbol, d) for label, d in config.items()}

        # strict - there must be data for every configured market date
        no_data = [df for df in data_by_label.values() if df is None]
        if len(no_data) > 0:
            raise DigestError(f'Missing market data. Check market data is available. {json.dumps(config)}')

        # strict - there must be no duplicate market dates
        dates = [df['MKT_DATE'].values[0] for df in data_by_label.values()]
        if len(set(dates)) != len(dates):
            raise DigestError(f'Duplicate market data. Check market data is available. {json.dumps(config)}')

        # stick the label on each data frame and concat them all
        for label, df in data_by_label.items():
            df['LABEL'] = label

        digest = pd.concat(data_by_label.values())
        digest = digest.sort_values(by=['MKT_DATE'], ascending=False)

        # create digest columns, comparing everything to as of date
        as_of_close = as_of_data.iloc[0]['MKT_CLOSE']
        as_of_vol = as_of_data.iloc[0]['MKT_VOLUME']
        digest['CLOSE'] = digest['MKT_CLOSE'].apply(lambda x: digest_str(x, as_of_close))
        digest['VOLUME'] = digest['MKT_VOLUME'].apply(lambda x: digest_str(x, as_of_vol, ballpark_value=True))

        # reduce columns
        digest = digest[['LABEL', 'CLOSE', 'VOLUME']]

        #
        # create the digest - this is a 'tall' to 'wide' transform.
        # Example Input:
        #   DATE        PRICE   VOLUME
        #   2019-12-02  100     5000
        #   2019-12-03  200     6000
        #
        # Example Output:
        #   MEASURE     2019-12-01      2019-12-03
        #   PRICE       100             200
        #   VOLUME      5000            6000
        #
        metrics = ['CLOSE', 'VOLUME']
        digest = digest.melt(
            id_vars='LABEL',
            value_vars=metrics,
            var_name='METRIC'
        )
        digest = digest.pivot(index='METRIC', columns='LABEL', values='value')

        # prefix the as-of-date values
        as_of_label = datetime.datetime.strftime(as_of_date, '%a %d %b %y')
        as_of_close = f'{as_of_close:,.0f}'
        as_of_vol = ballpark.business(as_of_vol)
        digest.insert(loc=0, column=as_of_label, value=[as_of_close, as_of_vol])

        # prefix the symbol
        digest.insert(loc=0, column='SYMBOL', value=[symbol]*len(metrics))

        digest = digest.reset_index()
        return digest

    @staticmethod
    def alpha_vantage_download(symbol: str, api_key: str, historical=False):
        def str_to_date(d: str):
            if len(d) != 10:
                raise ValueError('Expected string of length 10 (yyyy-mm-dd)')
            return datetime.date(int(d[0:4]), int(d[5:7]), int(d[8:10]))

        def to_pandas(vantage_data, ts_name):
            pandas_data = []
            for date, item in vantage_data[ts_name].items():
                pandas_data.append({
                    'MKT_SYMBOL': symbol,
                    'MKT_DATE': str_to_date(date),
                    'MKT_OPEN': float(item['1. open']),
                    'MKT_HIGH': float(item['2. high']),
                    'MKT_LOW': float(item['3. low']),
                    'MKT_CLOSE': float(item['4. close']),
                    'MKT_VOLUME': float(item['5. volume'])
                })
            return pd.DataFrame(pandas_data)

        def get_vantage_data(vantage_function):
            url = "https://www.alphavantage.co/query"
            params = urllib.parse.urlencode({
                'function': vantage_function,
                'outputsize': 'full' if historical else 'compact',
                'symbol': symbol,
                'apikey': api_key})
            response = requests.get(url, params)
            if not response.ok:
                raise RuntimeError(f'Alpha Vantage request failed with status ({response.status_code}). '
                                   f'URL was {url} and params were {params}')
            doc = response.json()
            return doc

        return to_pandas(get_vantage_data('TIME_SERIES_DAILY'), 'Time Series (Daily)')
