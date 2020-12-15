"""get_options.py pulls data on the stocks listed in tickers.csv and gets the options data on each, combined into one
dataframe then exported as a csv file. It requires a weekly update from update_price_funcs.py as well """

import os
import json
import csv
import datetime
from requests import exceptions
import pandas as pd
import yfinance as yf
from tqdm import tqdm
from pytz import utc

__author__ = "Charles Beach"
__credits__ = "Charles Beach"
__license__ = "MIT"
__version__ = "0.8"
__maintainer__ = "Charles Beach"
__email__ = "beachc15@gmail.com"
__status__ = "Development"


def keep_index(my_list_of_options):
    """ Keep only the middle 50% of rows in the dataframe
    The reasoning for this is that the most in the money and most out of the money options will
    not provide any consistent results"""
    my_index = [0, 1]
    output_list = {}
    for i in my_index:
        this_obj = my_list_of_options[i]
        this_index = len(this_obj.index)
        sixth_of_index = int(this_index / 6)
        two_sixths = int(sixth_of_index * 2)
        four_sixths = int(sixth_of_index * 4)
        output_list[i] = this_obj[two_sixths:four_sixths]
    return output_list


def main(ticker_path='/home/pi/python_projects/python_prod/rasbpi_options/tickers.csv'):
    i = 0
    with open(ticker_path) as f:
        for tickers in csv.reader(f):
            print(tickers)
    errors = 0
    total = 0
    this_time_export_data = {}
    now = datetime.datetime.now(tz=utc).strftime('%m_%d_%y-%H:%M')
    for ticker in tqdm(tickers):
        this_ticker_export_data = {}
        this_yf_object = yf.Ticker(ticker)
        expiration_list = this_yf_object.options
        for expiration in expiration_list[0:10]:
            this_expiration_export_data = {}
            total += 1
            try:
                this_yf_object_option_chain = this_yf_object.option_chain(
                    expiration)
                this_yf_object_option_chain_indexed = keep_index(
                    this_yf_object_option_chain)
                this_expiration_export_data['call'] = pd.DataFrame(this_yf_object_option_chain_indexed[0]).drop(
                    ['percentChange', 'inTheMoney', 'currency', 'contractSize'], axis=1)
                this_expiration_export_data['call']['optType'] = 'c'
                this_expiration_export_data['put'] = pd.DataFrame(this_yf_object_option_chain_indexed[1]).drop(
                    ['percentChange', 'inTheMoney', 'currency', 'contractSize'], axis=1)
                this_expiration_export_data['put']['optType'] = 'p'
                this_ticker_export_data[expiration] = this_expiration_export_data
                df_out = this_expiration_export_data['call'].append(
                    this_expiration_export_data['put'])
                df_out['myDateTime'] = datetime.datetime.now(tz=utc)

                expiration_split = list(map(int, expiration.split('-')))
                df_out['expiration'] = datetime.date(year=expiration_split[0], month=expiration_split[1],
                                                     day=expiration_split[2])
                df_out['uid'] = df_out['contractSymbol'] + '-' + now
                df_out = df_out.set_index('uid')

            except json.decoder.JSONDecodeError:
                errors += 1
                if errors >= 100:
                    print(errors)
        # This is my way of making sure my CSV file has headers while not trying to hold the whole thing in memory
        # TODO add below lines to the try statement to avoid the chance of an empty df_out
        i += 1
        if i == 1:
            this_time_export_data = df_out
        else:
            this_time_export_data = this_time_export_data.append(df_out)
    add_price(this_time_export_data, tickers)
    return this_time_export_data


def add_price(df, tickers):
    def add_ticker(df_):
        df_['ticker'] = df_['contractSymbol'].apply(
            lambda x: ''.join([i for i in x[0:4] if not i.isdigit()]))
        return df_

    def time_to_strike_df(df_):
        """very specialized: do not use"""
        cache = {}

        def time_to_strike(expiration_dt, current_dt):
            """returns the amount of time left until expiration as a fraction of a year (365 days)"""
            from datetime import time, datetime
            if (expiration_dt, current_dt) not in cache:
                time_delta = datetime.combine(
                    expiration_dt, time(hour=10), tzinfo=utc) - current_dt
                seconds = time_delta.seconds
                seconds_as_part_of_day = seconds / 86400
                time_delta_float = round(
                    (float(time_delta.days) + seconds_as_part_of_day) / 365, 6)
                cache[(expiration_dt, current_dt)] = time_delta_float
            else:
                time_delta_float = cache[(expiration_dt, current_dt)]
            return time_delta_float

        df_['timeUntilExpiration'] = df_.apply(
            lambda x: time_to_strike(x['expiration'], x['myDateTime']), axis=1)
        df_['pctPriceDiff'] = None
        return df_

    df = add_ticker(df)

    df['currentPriceDay'] = None
    df['stockVolumeDay'] = None
    df = time_to_strike_df(df)
    return df


def run_program(export_dir_path='/home/pi/Documents/data/options_daily'):
    current = datetime.datetime.now(tz=utc)
    weekno = datetime.datetime.today().weekday()
    open_time = datetime.time(hour=14, minute=30)
    close_time = datetime.time(hour=21, minute=30)
    if open_time < current.time() < close_time and weekno < 5:
        str_dt = current.strftime('%m_%d_%y')

        if not os.path.exists(f'{export_dir_path}/{str_dt}/'):
            os.makedirs(f'{export_dir_path}/{str_dt}/')
        file_name = current.strftime('%m_%d_%y-%H:%M')

        inp = main()

        with open(f'{export_dir_path}/{str_dt}/{file_name}.csv', 'w')as f:
            inp.to_csv(f)
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

if __name__ == "__main__":
    import http
    import urllib3
    error_count = 0
<<<<<<< Updated upstream
    try: 
=======
    max_err = 3
    try:
>>>>>>> Stashed changes
        run_program()
    except exceptions.ChunkedEncodingError:
        print("Encountered ChunkedEncodingError")
        if error_count < max_err:
            run_program()
            error_count += 1
    except http.client.IncompleteRead:
        print("Encountered Incomplete Read")
        if error_count < max_err:
            run_program()
            error_count += 1
    except urllib3.exceptions.ProtocolError:
        print("Encountered Protocol Error")
        if error_count < max_err:
            run_program()
            error_count += 1
            
