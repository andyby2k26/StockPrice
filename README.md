This project is designed to be a simple demonstration using multiple technologies including Docker, Postgres, SQL and different public APIs.

Below is an example of the Config.ini file used by ConfigParser in stockprice.py
Additional tickers can be added in comma-delimited list under general->tickers
Your own API keys will need to be supplied for both [Tradermade](https://tradermade.com/) and [Finnhub](https://finnhub.io/) both of which are free to sign up for

### Config.ini
```
[general]
tickers=["AMZN", "TSLA", "NFLX"]

[tradermadeAPI]
url=https://marketdata.tradermade.com/api/v1/timeseries
api_key=<YOUR_API_KEY_HERE>

[finnhubAPI]
company_profile_url = https://finnhub.io/api/v1/stock/profile2
api_key= <YOUR_API_KEY_HERE>

[postgres]
hostname = stock-postgres
database = stocks
username = myuser
pwd = mypassword
port_id = 5432
```
