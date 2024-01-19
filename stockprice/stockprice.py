import requests
import psycopg2
import sys
import json
import configparser
import datetime


#define config parser
config = configparser.ConfigParser()
config.read('config.ini')


class StockPriceAPI:
    def timeseriesAPI(ticker, start_date, end_date):
        url = config['tradermadeAPI']['url']
        api_key = config['tradermadeAPI']['api_key']
        params = {'api_key': api_key, 'currency':ticker, 'format':'records', 'start_date':start_date, 'end_date':end_date, 'interval':'hourly', 'period':'1'}
        r = requests.get(url=url, params=params)
        return r.json()
    
class FinnhubAPI:
    def companyprofileAPI(ticker):
        url = config['finnhubAPI']['company_profile_url']
        api_key = config['finnhubAPI']['api_key']
        params = {'token': api_key, 'symbol':ticker}
        r = requests.get(url, params=params)
        return r.json()

class PostgresDb:
    def __init__(self):
        self.hostname = config['postgres']['hostname']
        self.database = config['postgres']['database']
        self.username = config['postgres']['username']
        self.pwd = config['postgres']['pwd']
        self.port_id = config['postgres']['port_id']
        self.conn = None
        
    def connect(self):
        try:
            self.conn = psycopg2.connect(
            host=self.hostname, 
            database=self.database, 
            user=self.username, 
            password=self.pwd, 
            port=self.port_id
            )

            print(f"Connected to {self.database} Database")
        
        except Exception as error:
            print(error)
            sys.exit(1)
    
    def disconnect(self):
        if self.conn:
            self.conn.close()
            print(f"Disconnected from {self.database} database!")
    
    def execute_query(self, query, params=None, results=False):
        if self.conn is None:
            print("Not connected to a database")
            return
        
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if results:
                    results = cursor.fetchall()   
                    self.conn.commit()
                    return results
                else:
                    self.conn.commit()
                    return 
            
        except Exception as e:
            print(f"Error executing the query: {e}")

    def truncate_query(self, table):
        if self.conn is None:
            print("Not connected to a database")
            return
        
        try:
            with self.conn.cursor() as cursor:
                
                cursor.execute(f"TRUNCATE TABLE {table};")
  
                self.conn.commit()
                return
            
        except Exception as e:
            print(f"Error executing the query: {e}")


    def execute_insert_query(self, query, params=None):
        if self.conn is None:
            print("Not connected to a database")
            return
        
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)                  
                self.conn.commit()
                #print("row inserted")
                return
        except Exception as e:
            print(f"Error executing the query: {e}")
        

#connecting to DB
db = PostgresDb()
db.connect()

#define queries
insert_query_stock_price = "INSERT INTO raw_stock_prices (ticker, date, open, close, high, low) VALUES (%s,%s,%s,%s,%s,%s);"
insert_query_company_profile = "INSERT INTO raw_company_profile (ticker, county, exchange, ipo, marketcapitalization, name, phone, shareoutstanding, weburl, finnhubindustry) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

#Truncate Raw staging tables
db.truncate_query('raw_stock_prices')
db.truncate_query('raw_company_profile')

tickers = json.loads(config.get("general","tickers"))

for ticker in tickers:
    #get start date for current ticker for delta load
    start_date_query = f"SELECT MAX(date) FROM stock_prices WHERE ticker = '{ticker}';"
    start_date = db.execute_query(start_date_query, results=True)


    #Get company profile
    company_profile = FinnhubAPI.companyprofileAPI(ticker)
    data_to_insert = (company_profile["ticker"], company_profile["country"],company_profile["exchange"],company_profile["ipo"],company_profile["marketCapitalization"],company_profile["name"],company_profile["phone"],company_profile["shareOutstanding"],company_profile["weburl"],company_profile["finnhubIndustry"])
    db.execute_insert_query(insert_query_company_profile, data_to_insert)


    if start_date is None or start_date[0][0] == None:
        d = datetime.datetime.now() - datetime.timedelta(weeks=4)
        start_date = d.strftime( "%Y-%m-%d")
    else:
        start_date = start_date[0][0].strftime("%Y-%m-%d")
    
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    json_data = StockPriceAPI.timeseriesAPI(ticker=ticker, start_date=start_date, end_date=end_date)
    x = 1
    
    for quote in json_data['quotes']:
        data_to_insert = (json_data["instrument"], quote['date'], quote['open'], quote['close'], quote['high'], quote['low'])
        db.execute_insert_query(insert_query_stock_price, data_to_insert)
        x += 1
    print(f"{x} Rows Inserted for ticker {ticker}")

db.execute_query("call scd1_company_profile_update()",results=False)
db.execute_query("call stock_price_insert()",results=False)

db.disconnect()
