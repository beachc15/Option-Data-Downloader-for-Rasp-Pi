"""Updates the previous week's (monday through current day) 'current price', 'current volume', and 'percent
difference strike from price' columns on dataframes generated from get_options.py

Step by step:
This script figures out the current date
figures out monday of this week
creates a list of all files written to the directories associated with those dates
in each file, adds the columns mentioned above with accurate data

reasoning: The minute-by-minute pricing information available thorugh yfinance is not available immediately 100% of
the time. To make sure we dont add a bunch of random 'null' values to our dataframe, this program just adss the whole
week at once. """
import csv
import warnings
import datetime
import pandas as pd
from pytz import utc

__author__ = "Charles Beach"
__credits__ = "Charles Beach"
__license__ = "MIT"
__version__ = "0.8"
__maintainer__ = "Charles Beach"
__email__ = "beachc15@gmail.com"
__status__ = "Development"


def main():
    """
    Attempts to update dataframes for previous week by adding additional data such as the pct change needed to reach
    strike from current stock price, current stock price, current volume, etc.
    :return:
    """

    def get_dates():
        """
        Finds today's date and finds monday of this week. Returns both as datetime objects
        :return:
        """
        # find latest date in directory
        # take that date (end date), convert to datetime object, check day of week, find monday of that week,
        # set monday as start time
        # using CRONTAB this should only be run on fridays but this will work for error handling
        friday_check = False
        friday_int = 5

        cur_date = datetime.datetime.now(tz=utc).date()
        year_, week_, weekday_ = cur_date.isocalendar()

        # check day of week. If the day is not friday then throw a warning but continue running.
        if weekday_ != friday_int:
            if weekday_ > friday_int:
                # it is a weekend. Set day to Friday
                weekday_ = friday_int
                cur_date = datetime.date.fromisocalendar(year=year_, week=week_, day=weekday_)
                print(cur_date)
            warnings.warn(
                "Program not being run on a Friday. Please correct CRONTAB settings.",
                UserWarning,
                stacklevel=1
                )

        else:
            friday_check = True

        # Monday is known as integer 1 in the isocalendar format
        this_monday_date = datetime.date.fromisocalendar(year=year_, week=week_, day=1)

        # At this point we have our timeframe with cur_date being the end date and this_monday_date being the start
        return this_monday_date, cur_date

    def peruse_dir(start_, end_, my_dir_path="/home/pi/Documents/data/options_daily"):
        """
        Finds every file within the directories labeled within the start and end date parameters.

        :param my_dir_path: top level directory to walk throught
        :param end_: datetime.date object of start date to find
        :param start_: datetime.date object of start date to find
        :return files_list: List of files written from the top level dir path.
        """
        import os
        # set to parent folder of your data repository where every subfolder is labeled with the date of record
        files_list = []

        # for loop for each of the dates between start and end
        year, week, week_no_start = start_.isocalendar()
        _, _, week_no_end = end_.isocalendar()
        # below we create the list of dates to run our walk function on
        date_list = [datetime.date.fromisocalendar(year=year, week=week, day=x).strftime('%m_%d_%y') for x in range(week_no_start, week_no_end + 1)]
        # ****** Get list of files *******
        for date in date_list:
            date_path = f'{my_dir_path}/{date}'
            try:
                os.chdir(date_path)
                for root, dirs, files in os.walk(".", topdown=False):
                    print(root)
                    for name in files:
                        files_list.append(os.path.join(date_path, name))
            except FileNotFoundError:
                warnings.warn(f'Dir not found \'{date_path}\'.', RuntimeWarning)
                pass

        # Checked this function on a windows machine. Have yet to check on my actual raspberry pi
        print('***********************\n\n')
        print('file names')
        print(files_list)
        print('\n\n***********************')
        # first file should be a directory titled as the start_ date as a string in format(%m_%d_%y)
        # run a list for all datetime dates in between start and end, convert them to string and see if theres a
        return files_list

    def get_yfinance(start_, end_):
        """
        looks up the prices of stocks and the current volume of those stocks at a 1 minute interval between the start
        and end date specified in the args.
        :param start_:
        :param end_:
        :return:
        """
        import yfinance as yf
        dir_str = '/home/pi/python_projects/python_prod/rasbpi_options/tickers.csv'
        # dir_str = 'tickers.csv'

        # Get ticker list
        with open(dir_str) as f:
            for tickers_ in csv.reader(f):

                # Weirdly this works for instantiating the tickers object. I know its inefficient but meh.
                print(tickers_)

        # Download data from yfinance
        df_out = yf.download(tickers=tickers_, start=start_, end=end_, interval='1m')
        return df_out[['Adj Close', 'Volume']]

    def alter_csv(price_vol_df, file_str_):
        """
        IMPORTANT: This function actually opens and (potentially permanently) alters the file that is passed into it.
        :param price_vol_df: datetime.date Object
        :type file_str_: str
        :return:
        """
        # data function
        def price_delta_in_pct(strike, current_price):
            """
            Finds the percent change necessary from current price to reach strike price
            :param strike:
            :param current_price:
            :return:
            """
            round_int = 7
            pct_change = (round((strike - current_price) / current_price), round_int)
            return pct_change

        def get_time(file_str__):
            """
            Get recorded time from the file_str file name
            :param file_str__:
            :return:
            """
            hour_and_minute = list(map(int, (file_str__.split('-')[-1].split('.')[0].split(':'))))
            time_obj = datetime.time(hour=hour_and_minute[0], minute=hour_and_minute[1])
            return time_obj

        confirmed_match = False
        df_index_col = 'uid'

        df = pd.read_csv(file_str_, index_col=df_index_col)
        # TODO add some check to see if this dataframe has already been altered (just in case)
        #   honestly no this is dumb to check. I will leave this in just in case I change my mind but if its there whats
        #   the harm in just overwriting it?
        #   peak efficiency is too expensive for me

        # Get recorded time from the file_str file name
        my_time = get_time(file_str_)

        # Find the matching time from the yfinance csv dataframe that corresponds with the time stored as "my_time"
        try:
            for ind in price_vol_df.index:
                ind_match = ind.astimezone(utc)
                if ind_match.time() == my_time:
                    # confirmed_match is stored as a UNIX timestamp (I think)
                    # TODO confirm line 122
                    confirmed_match = ind

            # confirmed_match is the index of the row in the stock prices dataframe which we need to use
            # I should make the next part a "try.. except" method but I want to see what happens this way first

            #if not confirmed_match:
           #     raise KeyError(f'confirmed_match did not find a match with time {my_time}')
            adj_close_dict = price_vol_df.loc[confirmed_match]['Adj Close'].to_dict()
            volume_dict = price_vol_df.loc[confirmed_match]['Volume'].to_dict()

            # **Transformation**
            df['currentPriceDay'] = df['ticker'].apply(lambda x: adj_close_dict.get(x))
            df['stockVolumeDay'] = df['ticker'].apply(lambda x: volume_dict.get(x))
            df['pctPriceDiff'] = df.apply(lambda x: price_delta_in_pct(x['strike'], x['currentPriceDay']), axis=1)

            df.to_csv(path_or_buf=file_str_)
        except ValueError:
            pass
        except KeyError:
            pass

    start, end = get_dates()
    file_list = peruse_dir(start_=start, end_=end)
    df_prices = get_yfinance(start_=start, end_=end)

    for file_str in file_list:
        print(file_str)
        alter_csv(df_prices, file_str)

    # at this point we can start checking each file and altering it
    # As of right now I think I am going to pass it into a for loop for each of the file strings in file_list
    # and pass a function that actually goes in and makes the required alteration of the CSV.
    #       I won't be able to bug-check this on my windows machine


if __name__ == '__main__':
    main()
"""Updates the previous week's (monday through current day) 'current price', 'current volume', and 'percent
difference strike from price' columns on dataframes generated from get_options.py

Step by step:
This script figures out the current date
figures out monday of this week
creates a list of all files written to the directories associated with those dates
in each file, adds the columns mentioned above with accurate data

reasoning: The minute-by-minute pricing information available thorugh yfinance is not available immediately 100% of
the time. To make sure we dont add a bunch of random 'null' values to our dataframe, this program just adss the whole
week at once. """
import csv
import warnings
import datetime
import pandas as pd
from pytz import utc

__author__ = "Charles Beach"
__credits__ = "Charles Beach"
__license__ = "MIT"
__version__ = "0.8"
__maintainer__ = "Charles Beach"
__email__ = "beachc15@gmail.com"
__status__ = "Development"


def main():
    """
    Attempts to update dataframes for previous week by adding additional data such as the pct change needed to reach
    strike from current stock price, current stock price, current volume, etc.
    :return:
    """

    def get_dates():
        """
        Finds today's date and finds monday of this week. Returns both as datetime objects
        :return:
        """
        # find latest date in directory
        # take that date (end date), convert to datetime object, check day of week, find monday of that week,
        # set monday as start time
        # using CRONTAB this should only be run on fridays but this will work for error handling
        friday_check = False
        friday_int = 5

        cur_date = datetime.datetime.now(tz=utc).date()
        year_, week_, weekday_ = cur_date.isocalendar()

        # check day of week. If the day is not friday then throw a warning but continue running.
        if weekday_ != friday_int:
            if weekday_ > friday_int:
                # it is a weekend. Set day to Friday
                weekday_ = friday_int
                cur_date = datetime.date.fromisocalendar(
                    year=year_, week=week_, day=weekday_)
                print(cur_date)
            warnings.warn(
                "Program not being run on a Friday. Please correct CRONTAB settings.",
                UserWarning,
                stacklevel=1
            )

        else:
            friday_check = True

        # Monday is known as integer 1 in the isocalendar format
        this_monday_date = datetime.date.fromisocalendar(
            year=year_, week=week_, day=1)

        # At this point we have our timeframe with cur_date being the end date and this_monday_date being the start
        return this_monday_date, cur_date

    def peruse_dir(start_, end_, my_dir_path="/home/pi/Documents/data/options_daily"):
        """
        Finds every file within the directories labeled within the start and end date parameters.

        :param my_dir_path: top level directory to walk throught
        :param end_: datetime.date object of start date to find
        :param start_: datetime.date object of start date to find
        :return files_list: List of files written from the top level dir path.
        """
        import os
        # set to parent folder of your data repository where every subfolder is labeled with the date of record
        files_list = []

        # for loop for each of the dates between start and end
        year, week, week_no_start = start_.isocalendar()
        _, _, week_no_end = end_.isocalendar()
        # below we create the list of dates to run our walk function on
        date_list = [datetime.date.fromisocalendar(year=year, week=week, day=x).strftime(
            '%m_%d_%y') for x in range(week_no_start, week_no_end + 1)]
        # ****** Get list of files *******
        for date in date_list:
            date_path = f'{my_dir_path}/{date}'
            try:
                os.chdir(date_path)
                for root, dirs, files in os.walk(".", topdown=False):
                    print(root)
                    for name in files:
                        files_list.append(os.path.join(date_path, name))
            except FileNotFoundError:
                warnings.warn(f'Dir not found \'{date_path}\'.', RuntimeWarning)
                pass

        # Checked this function on a windows machine. Have yet to check on my actual raspberry pi
        print('***********************\n\n')
        print('file names')
        print(files_list)
        print('\n\n***********************')
        # first file should be a directory titled as the start_ date as a string in format(%m_%d_%y)
        # run a list for all datetime dates in between start and end, convert them to string and see if theres a
        return files_list

    def get_yfinance(start_, end_):
        """
        looks up the prices of stocks and the current volume of those stocks at a 1 minute interval between the start
        and end date specified in the args.
        :param start_:
        :param end_:
        :return:
        """
        import yfinance as yf
        dir_str = '/home/pi/python_projects/python_prod/rasbpi_options/tickers.csv'
        # dir_str = 'tickers.csv'

        # Get ticker list
        with open(dir_str) as f:
            for tickers_ in csv.reader(f):

                # Weirdly this works for instantiating the tickers object. I know its inefficient but meh.
                print(tickers_)

        # Download data from yfinance
        df_out = yf.download(tickers=tickers_, start=start_,
                             end=end_, interval='1m')
        return df_out[['Adj Close', 'Volume']]

    def alter_csv(price_vol_df, file_str_):
        """
        IMPORTANT: This function actually opens and (potentially permanently) alters the file that is passed into it.
        :param price_vol_df: datetime.date Object
        :type file_str_: str
        :return:
        """
        # data function
        def price_delta_in_pct(strike, current_price):
            """
            Finds the percent change necessary from current price to reach strike price
            :param strike:
            :param current_price:
            :return:
            """
            round_int = 7
            pct_change = (
                round((strike - current_price) / current_price), round_int)
            return pct_change

        def get_time(file_str__):
            """
            Get recorded time from the file_str file name
            :param file_str__:
            :return:
            """
            hour_and_minute = list(
                map(int, (file_str__.split('-')[-1].split('.')[0].split(':'))))
            time_obj = datetime.time(
                hour=hour_and_minute[0], minute=hour_and_minute[1])
            return time_obj

        confirmed_match = False
        df_index_col = 'uid'

        df = pd.read_csv(file_str_, index_col=df_index_col)
        # TODO add some check to see if this dataframe has already been altered (just in case)
        #   honestly no this is dumb to check. I will leave this in just in case I change my mind but if its there whats
        #   the harm in just overwriting it?
        #   peak efficiency is too expensive for me

        # Get recorded time from the file_str file name
        my_time = get_time(file_str_)

        # Find the matching time from the yfinance csv dataframe that corresponds with the time stored as "my_time"
        try:
            for ind in price_vol_df.index:
                ind_match = ind.astimezone(utc)
                if ind_match.time() == my_time:
                    # confirmed_match is stored as a UNIX timestamp (I think)
                    # TODO confirm line 122
                    confirmed_match = ind

            # confirmed_match is the index of the row in the stock prices dataframe which we need to use
            # I should make the next part a "try.. except" method but I want to see what happens this way first

            # if not confirmed_match:
           #     raise KeyError(f'confirmed_match did not find a match with time {my_time}')
            adj_close_dict = price_vol_df.loc[confirmed_match]['Adj Close'].to_dict(
            )
            volume_dict = price_vol_df.loc[confirmed_match]['Volume'].to_dict()

            # **Transformation**
            df['currentPriceDay'] = df['ticker'].apply(
                lambda x: adj_close_dict.get(x))
            df['stockVolumeDay'] = df['ticker'].apply(
                lambda x: volume_dict.get(x))
            df['pctPriceDiff'] = df.apply(lambda x: price_delta_in_pct(
                x['strike'], x['currentPriceDay']), axis=1)

            df.to_csv(path_or_buf=file_str_)
        except KeyError:
            pass

    start, end = get_dates()
    file_list = peruse_dir(start_=start, end_=end)
    df_prices = get_yfinance(start_=start, end_=end)

    for file_str in file_list:
        print(file_str)
        alter_csv(df_prices, file_str)

    # at this point we can start checking each file and altering it
    # As of right now I think I am going to pass it into a for loop for each of the file strings in file_list
    # and pass a function that actually goes in and makes the required alteration of the CSV.
    #       I won't be able to bug-check this on my windows machine


if __name__ == '__main__':
    main()
