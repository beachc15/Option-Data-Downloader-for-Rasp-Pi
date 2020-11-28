import pandas as pd
import yfinance as yf
from tqdm import tqdm
import json
import csv
import datetime
from pytz import utc
import os


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
    with open('/home/pi/python_projects/python_prod/rasbpi_options/tickers.csv') as f:
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
        # anything with minute dependent data has to be thrown out as all data is delayed by 15 minutes.
        # Instead this will be run on a seperate job
        # df_['pctPriceDiff'] = df_.apply(lambda x: price_delta_in_pct(
        #     x['strike'], x['currentPriceDay']), axis=1)
        df_['pctPriceDiff'] = None
        return df_

    '''
    df2 = yf.download(tickers, interval='5m', period='5d').tail(1)
    print(df2)
    prices = df2['Adj Close'].to_dict('list')
    volume = df2['Volume'].to_dict('list')
    for price in prices:
        prices[price] = prices[price][0]
        volume[price] = volume[price][0]
        '''

    df = add_ticker(df)

    '''
    df['currentPriceDay'] = df["ticker"].apply(lambda x: prices.get(x))
    df['stockVolumeDay'] = df["ticker"].apply(lambda x: volume.get(x))
    '''

    df['currentPriceDay'] = None
    df['stockVolumeDay'] = None
    df = time_to_strike_df(df)
    return df


def run_program():
    current = datetime.datetime.now(tz=utc)
    weekno = datetime.datetime.today().weekday()
    open_time = datetime.time(hour=14, minute=30)
    close_time = datetime.time(hour=21, minute=30)
    if open_time < current.time() < close_time and weekno < 5:
        str_dt = current.strftime('%m_%d_%y')
        my_path = '/home/pi/Documents/data/options_daily/'
        if not os.path.exists(f'{my_path}/{str_dt}/'):
            os.makedirs(f'{my_path}/{str_dt}/')
        file_name = current.strftime('%m_%d_%y-%H:%M')
        line = []

        inp = main()

        with open('/home/pi/Documents/data/check_file.csv', 'r') as fd:
            try:
                line = [x for x in csv.reader(fd)][0]
                print(line)
            except IndexError:
                line = [x for x in csv.reader(fd)]
            line.append(f'{file_name}.csv')
        with open('/home/pi/Documents/data/check_file.csv', 'w') as fd:
            if isinstance(line, list):
                print(line)
                line = list(map(str.strip, line))
                print(line)
                fd.write(', '.join(line))
            elif isinstance(line, str):
                fd.write(line)
            else:
                print('line was not string or list')
                print(f'line was of type {type(line)}')
                print(f'contents of line were {line}')

        with open(f'/home/pi/Documents/data/options_daily/{str_dt}/{file_name}.csv', 'w')as f:
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


run_program()
