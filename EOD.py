"""EOD.py gets the EndOfDay prices for the day. It looks at each stock listed in tickers.csv then pulls the
minute-by-minute data throughout the whole day on the stock through yfinance. It then saves it in a csv form"""
import datetime
import yfinance as yf
import csv
from pytz import utc
from update_price_funcs import get_dates

__author__ = "Charles Beach"
__credits__ = "Charles Beach"
__license__ = "MIT"
__version__ = "0.8"
__maintainer__ = "Charles Beach"
__email__ = "beachc15@gmail.com"
__status__ = "Development"


def end_of_day_collection():
    def fix_names(my_df):
        """Designed to change the names of each column from multiIndex to tuple"""
        out_col = []
        columns = my_df.columns
        for col in columns:
            out_col.append(str(col))

        my_df.columns = out_col
        return my_df

    ticker_str = '/home/pi/python_projects/python_prod/rasbpi_options/tickers.csv'
    current = datetime.datetime.now(tz=utc)
    # weekno = datetime.datetime.today().weekday()
    year_, week_, weekday_ = current.isocalendar()
    if (21 <= current.hour < 24) and weekday_ < 6:
        dt = current.strftime('%Y-%m-%d')

        with open(ticker_str) as f:
            for tickers in csv.reader(f):
                _ = tickers
        # export = {'date': dt, 'data': yf.download(tickers, interval='5m', start=dt).to_json()}
        open_str = f"/home/pi/Documents/data/EOD_prices/week-{week_}_{year_}.csv"
        start_, end_ = get_dates()
        with open(open_str, 'w') as f:
            df = yf.download(tickers, interval='5m', start=start_, end=end_)
            df = fix_names(df)
            df.to_csv(f)
        print()
        print()
        print('***********************')
        print()
        print(f'Successfully output to {open_str}')
        print()
        print('***********************')
        print()
        print()
    else:
        print(print(f'Will not run EOD as time is currently: {current}'))
        print('***********************')
        print(f'weekno: {weekday_}')
        print(f'Current Hour: {current.hour}')
        print('***********************')


if __name__ == '__main__':
    end_of_day_collection()
