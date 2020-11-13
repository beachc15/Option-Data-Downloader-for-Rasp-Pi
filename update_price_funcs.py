# As stock data does not update on yfinance for 15 minutes after publishing this script
# will look at our csv upload from 15 minutes ago and update its values accordingly
import csv
import pandas as pd


def main():
    file_path_check_file = '/home/pi/Documents/data/check_file.csv'
    file_path_edit = '/home/pi/Documents/data/options_daily/'
    files_to_fix = []
    files_to_keep = []
    # get the list of files to fix
    # files should be seperated by a line with the format {%m_%d_%y-%H:%M}.csv
    with open(file_path_check_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            files_to_fix.append(row)
    try:
        for fn in files_to_fix[0]:
            fn = fn.strip()
            dir_name = fn.split('-')[0]
            file_path = file_path_edit + dir_name + '/' + fn
            df = pd.read_csv(file_path, index_col='uid')
            check = df.head(1)
            if check['currentPriceDay'].isnull().sum().sum() != 0:
                response = get_price_info(df, fn, dir_name)
                if response == 'keep':
                    files_to_keep.append(fn)
                else:
                    pass
        with open(file_path_check_file, 'w') as f:
            if isinstance(files_to_keep, list):
                print(files_to_keep)
                f.write(', '.join(files_to_keep))
                # csv.writer(f).writerow(', '.join(files_to_keep))
            elif isinstance(files_to_keep, str):
                f.write(files_to_keep)
                # csv.writer(f).writerow(files_to_keep)
            else:
                print("error with writer")
                print(f"files to keep was {files_to_keep}")
                print(f"and of type {type(files_to_keep)}")

    except IndexError:
        pass

def get_price_info(df_, file_name, dir_name_):
    from datetime import time
    from pytz import utc

    def get_yfinance(tickers):
        import yfinance as yf
        prices = yf.download(tickers=tickers, interval='1m', period='1d')
        return prices

    def price_delta_in_pct(strike, current_price):
        """finds the percent change necessary from current price to reach strike price"""
        pct_change = round((strike - current_price) / current_price, 7)
        return round(pct_change, 7)

    def get_stock_list():
        with open('/home/pi/python_projects/python_prod/rasbpi_options/tickers.csv', 'r') as fd:
            for tickers_ in csv.reader(fd):
                pass
        return tickers_

    # Instantiate the datetime object we will use to match times later
    actual_match = 0

    stock_list = get_stock_list()
    current_prices = get_yfinance(stock_list)
    time_as_str = list(map(int, file_name.split('.')[
        0].split('-')[-1].split(':')))
    time_obj = time(hour=time_as_str[0], minute=time_as_str[1])
    for ind in current_prices.index:
        ind_match = ind.astimezone(utc)
        if ind_match.time() == time_obj:
            actual_match = ind
            break
    try:
        adj_close_series = current_prices.loc[actual_match]['Adj Close']
        volume = current_prices.loc[actual_match]['Volume'].to_dict()
        adj_close = adj_close_series.to_dict()

        df_['currentPriceDay'] = df_['ticker'].apply(
            lambda x: adj_close.get(x))
        df_['stockVolumeDay'] = df_['ticker'].apply(lambda x: volume.get(x))
        df_['pctPriceDiff'] = df_.apply(lambda x: price_delta_in_pct(
            x['strike'], x['currentPriceDay']), axis=1)

        if adj_close_series.isnull().sum().sum() != 0:
            print('YAYYYYY')
            return 'keep'
        elif adj_close_series.isnull().sum().sum() == 0:
            df_.to_csv(f'/home/pi/Documents/data/options_daily/{dir_name_}/{file_name}')
            return 'remove'
    except KeyError:
        return 'keep'


if __name__ == '__main__':
    main()
