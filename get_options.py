import pandas as pd
import yfinance as yf
from tqdm import tqdm
import json
import csv
import datetime
from pytz import utc
from pymongo import MongoClient


def keep_index(myListofOptions):
	""" Keep only the middle 50% of rows in the dataframe
	The reasoning for this is that the most in the money and most out of the money options will
	not provide any consistent results"""
	my_index = [0, 1]
	output_list = {}
	for i in my_index:
		this_obj = myListofOptions[i]
		this_index = len(this_obj.index)
		sixth_of_index = int(this_index / 6)
		two_sixths = int(sixth_of_index * 2)
		four_sixths = int(sixth_of_index * 4)
		output_list[i] = this_obj[two_sixths:four_sixths]
	return output_list


def main():
	i = 0
	with open('tickers.csv') as f:
		for tickers in csv.reader(f):
			print(tickers)
	errors = 0
	total = 0
	this_time_export_data = {}
	for ticker in tqdm(tickers):
		this_ticker_export_data = {}
		this_yf_object = yf.Ticker(ticker)
		expiration_list = this_yf_object.options
		for expiration in expiration_list[0:9]:
			this_expiration_export_data = {}
			total += 1
			try:
				this_yf_object_option_chain = this_yf_object.option_chain(expiration)
				this_yf_object_option_chain_indexed = keep_index(this_yf_object_option_chain)
				this_expiration_export_data['call'] = pd.DataFrame(this_yf_object_option_chain_indexed[0]).drop(
					['percentChange', 'strike', 'inTheMoney', 'currency'], axis=1)
				this_expiration_export_data['call']['uid'] = (
							 this_expiration_export_data['call']['contractSymbol'] + '-' + datetime.datetime.now(
						tz=utc).strftime('%m_%d_%y-%H:%M'))
				this_expiration_export_data['put'] = pd.DataFrame(this_yf_object_option_chain_indexed[1]).drop(
					['percentChange', 'strike', 'inTheMoney', 'currency'], axis=1)
				this_expiration_export_data['put']['uid'] = (
							 this_expiration_export_data['call']['contractSymbol'] + '-' + datetime.datetime.now(
						tz=utc).strftime('%m_%d_%y-%H:%M'))
				this_ticker_export_data[expiration] = this_expiration_export_data

				df_out = this_expiration_export_data['call'].append(this_expiration_export_data['put'])
				df_out['myDateTime'] = datetime.datetime.now(tz=utc).strftime('%m_%d_%y-%H:%M')

			except json.decoder.JSONDecodeError:
				errors += 1
				if errors >= 100:
					print(errors)
		i += 1
		if i == 1:
			this_time_export_data = df_out
		else:
			this_time_export_data = this_time_export_data.append(df_out)

	return this_time_export_data


def run_program():
	current = datetime.datetime.now(tz=utc)
	weekno = datetime.datetime.today().weekday()
	if 0 < current.hour < 24 and weekno < 5:
		file_name = datetime.datetime.now(tz=utc).strftime('%m_%d_%y-%H:%M')
		inp = main()
		with open(f'~/Documents/data/options_daily/{file_name}.csv', 'w') as f:
			inp.to_csv(f, index= False)
		print('****************************************************************')
		print('*                                                              *')
		print(f'*\t pushed for {current.strftime("%H:%M")}')
		print('*                                                              *')
		print('****************************************************************')
	else:
		print('****************************************************************')
		print('*                                                              *')
		print('*             Will not run as the datetime is                  *')
		print(f'\t\t{current}')
		print(f'*             and the markets are not open                     *')
		print('*                                                               *')
		print('****************************************************************')


run_program()
