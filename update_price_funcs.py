# As stock data does not update on yfinance for 15 minutes after publishing this script
# will look at our csv upload from 15 minutes ago and update its values accordingly
import csv
import pandas as pd


def main():
    file_path_check_file = '/home/pi/Documents/data/check_file.csv'
    file_path_edit = '/home/pi/Documents/data/options_daily/'
    files_to_fix = []

    # get the list of files to fix
    # files should be seperated by a line with the format {%m_%d_%y-%H:%M}.csv
    with open(file_path_check_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            files_to_fix.append(row)

    for fn in files_to_fix[0]:
        fn = fn.strip()
        print(fn)
        dir_name = fn.split('-')[0]
        print('dir name: ', dir_name)
        file_path = file_path_edit + dir_name + '/' + fn
        print('file path: ', file_path)
        df = pd.read_csv(file_path, index_col='uid')
        check = df.head(1)
        print(df)
        if check['currentPriceDay'].isnull:
            get_price_info(df)
        else:
            files_to_fix.pop(fn)


def get_price_info(df_):
    print('got here')


if __name__ == '__main__':
    main()
