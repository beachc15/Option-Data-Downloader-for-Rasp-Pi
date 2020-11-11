import datetime
from pymongo import MongoClient
import yfinance as yf
import csv
from pytz import utc
import json


def end_of_day_collection():
	current = datetime.datetime.now(tz=utc)
	weekno = datetime.datetime.today().weekday()
	if (21 <= current.hour < 24) and weekno < 5:
		dt = current.strftime('%Y-%m-%d')
		with open('tickers.csv') as f:
			for tickers in csv.reader(f):
				_ = tickers
		export = {'date': dt, 'data': yf.download(tickers, interval='5m', start=dt).to_json()}
		with open(f'~/Documents/data/EOD_prices/{dt}.json', 'w') as f:
			json.dump(export, f)
		print()
		print()
		print('***********************')
		print()
		print(f'Successfully output to ~/Documents/data/EOD_prices/{dt}.json')
		print()
		print('***********************')
		print()
		print()
	else:
		print(print(f'Will not run EOD as time is currently: {current}'))
		print('***********************')
		print(f'weekno: {weekno}')
		print(f'Current Hour: {current.hour}')
		print('***********************')


end_of_day_collection()
