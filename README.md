### Local Setup

**1 Create Python Environment**
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
touch bin/config.ini
```

**2 Create Postgres DB on AWS Free Tier**

TODO

**3 Create Alpha Vantage API Key**

TODO

**4 Create Configuration File**

Populate bin/config.ini with the following

```
[DATABASE]
minconn=1
maxconn=2
host=<aws database hostname>
port=5432
user=<aws database username>
password=<aws database password>
database=<aws database name>

[ALPHA_VANTAGE]
api_key=<alpha vantage api key>
```

### Stocks Database and Daily Digest

**1 Load historical prices into database**

Incremental load can be started by removing --historical flag

```
(venv) ➜  financial-apps bin/stocks/load.py SPX --historical
2020-02-03 21:14:18,479 finapps.stocks.service INFO     [SPX] downloading historical data
2020-02-03 21:14:20,645 finapps.stocks.service INFO     [SPX] persisting 5053 records
2020-02-03 21:14:30,215 finapps.stocks.service INFO     [SPX] done
(venv) ➜  financial-apps 
```

**2 Create a performance digest!**

```
(venv) ➜  financial-apps bin/stocks/digest.py SPX 2018-11-01
2020-02-03 21:36:58,124 root         INFO     [SPX] min persisted date: 2000-01-03
2020-02-03 21:36:58,124 root         INFO     [SPX] max persisted date: 2020-02-03
2020-02-03 21:36:58,376 finapps.stocks.service INFO     [SPX] has no data for 2018-05-05, using closest available date 2018-05-04

 METRIC SYMBOL Thu 01 Nov 18             1d             1m             1y             3m             3y             6m
  CLOSE    SPX         2,740   2,712 (↑ 1%)  2,923 (↓ -6%)   2,579 (↑ 6%)  2,840 (↓ -3%)  2,104 (↑ 30%)   2,663 (↑ 2%)
 VOLUME    SPX         4.71G  5.11G (↓ -7%)  3.40G (↑ 38%)  3.81G (↑ 23%)  3.03G (↑ 55%)  3.76G (↑ 25%)  3.33G (↑ 41%)
 
2020-02-03 21:36:58,513 digest.py    INFO     Finished

```
