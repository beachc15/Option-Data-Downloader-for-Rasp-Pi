# Option-Data-Downloader-for-Rasp-Pi
Tool for building an options dataset written in Python for deployment on the Raspberry Pi. 

# Summary
  This repo was designed to build a large dataset of options collected over a certain interval for every weekday. It uses the list of stock symbols in `tickers.csv` and 
  the package [yfinance](https://pypi.org/project/yfinance/) to pull options data and store it in a CSV with a unique identifying string for each entry. All stocks, contracts,
  expirations and everything are stored in a single CSV for that time period which is saved to a directory including all of that days' entries. 
  
# Goal
  My goal with this project is to use machine learning to predict an option price some certain time in the future and then write an algorithm that trades based on the 
  ML program's findings while the data-puller continuously updates the models on a rolling bases
  
# Contact
  **Please, if you find this at all interesting, reach out to me [Beachc15@gmail.com](beachc15@gmail.com). I have been writing code for a few years now and haven't found many
  people with similar interests who like discussing things like this. I have a dream of attending a masters program in Computational Finance but I worry my lack of a formal
  coding/math background limits me.**

# TODO
 - Right now we are losing ~5% of each days data to the following [ChunkedEncodingError](https://github.com/beachc15/Option-Data-Downloader-for-Rasp-Pi/issues/2)
 - Add to-the-minute accurate pricing of the stock for the time that the option data was recorded
    - FYI the record datetime is used rather than the lastTradeDate because the record datetime is representative of the going price of the option, rather than the last price that was actuall paid for it.
    - I would just set it to record this at the time of the original data pull but sometimes the yfinance API doesn't update immediately. 
    - I am currently (11/28) working on a solution that updates for the whole week each friday. This would lighten the use of the API and speed up the iterative daily computation as it doesn't need to perform 2 web scraping operations for each interval.
