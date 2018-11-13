import datetime

import pandas as pd
import psycopg2.extras
import psycopg2.pool


class MarketDataDAO:
    """
    Simple DAO for storage of market data. Assumes use of postgres and pandas.
    """
    def __init__(self, pool: psycopg2.pool.AbstractConnectionPool):
        self.pool = pool
        self.schema = {
            'MKT_SYMBOL': 'str',
            'MKT_DATE': 'datetime64',
            'MKT_OPEN': 'float64',
            'MKT_HIGH': 'float64',
            'MKT_LOW': 'float64',
            'MKT_CLOSE': 'float64',
            'MKT_VOLUME': 'int64'
        }

    def select(self, symbol: str, lo_date: datetime.date, hi_date: datetime.date) -> pd.DataFrame:
        query = """
            SELECT MKT_SYMBOL, 
                   MKT_DATE, 
                   MKT_OPEN, 
                   MKT_HIGH, 
                   MKT_LOW, 
                   MKT_CLOSE, 
                   MKT_VOLUME 
            FROM MKT_DATA
            WHERE MKT_SYMBOL = %(symbol)s AND (MKT_DATE >= %(lo_date)s AND MKT_DATE <= %(hi_date)s)
        """
        params = {'symbol': symbol, 'lo_date': lo_date, 'hi_date': hi_date}
        with self.pool.getconn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    result = cur.fetchall()
                    df = pd.DataFrame(result, columns=self.schema.keys())
                    return MarketDataDAO.to_schema(df, self.schema)
            finally:
                self.pool.putconn(conn)

    def min_max_dates(self, symbol: str) -> (datetime.date, datetime.date):
        query = """
            SELECT MIN(MKT_DATE), MAX(MKT_DATE)
            FROM MKT_DATA
            WHERE MKT_SYMBOL = %(symbol)s
        """
        params = {'symbol': symbol}
        with self.pool.getconn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchone()
            finally:
                self.pool.putconn(conn)

    def insert(self, df: pd.DataFrame):
        if set(df.columns) != set(self.schema.keys()):
            raise RuntimeError(f'DataFrame columns must match schema columns {self.schema.keys()}. '
                               f'Actual cols {df.columns}')
        df = MarketDataDAO.to_schema(df, self.schema)
        query = """
            INSERT INTO MKT_DATA (
               MKT_SYMBOL, 
               MKT_DATE, 
               MKT_OPEN, 
               MKT_HIGH, 
               MKT_LOW, 
               MKT_CLOSE, 
               MKT_VOLUME
            ) VALUES (
                %(MKT_SYMBOL)s,
                %(MKT_DATE)s,
                %(MKT_OPEN)s,
                %(MKT_HIGH)s,
                %(MKT_LOW)s,
                %(MKT_CLOSE)s,
                %(MKT_VOLUME)s
            ) ON CONFLICT (MKT_SYMBOL,MKT_DATE) DO UPDATE SET 
                MKT_OPEN=excluded.MKT_OPEN,
                MKT_HIGH=excluded.MKT_HIGH,
                MKT_LOW=excluded.MKT_LOW,
                MKT_CLOSE=excluded.MKT_CLOSE,
                MKT_VOLUME=excluded.MKT_VOLUME 
            """
        params = df.to_dict(orient='records')
        with self.pool.getconn() as conn:
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, query, params)
                conn.commit()
            finally:
                self.pool.putconn(conn)

    @staticmethod
    def to_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """
        Convert dtypes in the given DataFrame to those specified in the schema.
        Unlikely to cover all edge cases but meets immediate needs.
        """
        target_cols = [c for c in df.columns if c in schema]
        for c in target_cols:
            expected_type = schema[c]
            df[c] = df[c].astype(expected_type)
        return df


def with_simple_pool(**kwargs):
    p = psycopg2.pool.SimpleConnectionPool(**kwargs)
    return MarketDataDAO(p)
