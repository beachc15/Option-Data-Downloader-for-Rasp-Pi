# Option-Data-Downloader-for-Rasp-Pi
Tool for building an options dataset written in Python for deployment on the Raspberry Pi. 

# Summary
  This repo was designed to build a large dataset of options collected over a certain interval for every weekday. It uses the list of stock symbols in `tickers.csv` and 
  the package [yfinance](https://pypi.org/project/yfinance/) to pull options data and store it in a CSV with a unique identifying string for each entry. All stocks, contracts,
  expirations and everything are stored in a single CSV for that time period which is saved to a directory including all of that days' entries. 
  
# Goal
  My goal with this project is to use machine learning to predict an option price some certain time in the future and then write an algorithm that trades based on the 
  ML program's findings while the data-puller continuously updates the models on a rolling bases
  
# Instructions

- Run ```git clone https://github.com/beachc15/Option-Data-Downloader-for-Rasp-Pi``` on your raspberry pi's CLI
- Set up Crontab to schedule jobs using ```crontab -e``` to open file
  - At the end of the file add the following two lines to schedule the two main files to run:
   ```
   */5 * * * 1-5 </somedir/python3.8 binary> /home/usr/<repo download location>/get_options.py
   33 21 * * 1-5 </somedir/python3.8 binary> /home/usr/<repo download location>/EOD.py
   0 21 * * 5 </somedir/python3.8 binary> /home/usr/<repo download location>/update_price_funcs.py
   ```
   *Note: These times are all set to UTC. I changed "21" to "15" for CST*
  - This crontab command will run ```get_options``` every 5 minutes monday through friday, runs ```update_price_funcs.py``` every friday at 9:00 PM and runs ```EOD.py``` at 9:33 PM UTC.
- Within get_options.py and EOD.py replace the location of [tickers.csv](https://github.com/beachc15/Option-Data-Downloader-for-Rasp-Pi/blob/master/tickers.csv) with the stocks you want to track
  - Also update the ```my_path``` string in the ```main``` function at the bottom to where you want to store your data
- At this point it should run without a hitch. Please reach out if any issues.
  
# Contact
  **Please, if you find this at all interesting, reach out to me [Beachc15@gmail.com](beachc15@gmail.com). I have been writing code for a few years now and haven't found many
  people with similar interests who like discussing things like this. I have a dream of attending a masters program in Computational Finance but I worry my lack of a formal
  coding/math background limits me.**
  
  Also, track my progress on my repo where I work with these files over on [daily option analysis](https://github.com/beachc15/daily-option-analysis)
  
# Next Steps 
- Create Pip release and OOP conversion so this can be deployed in a few lines of code on a web server/home computer or raspberry pi
- Save data only when a change is detected to save space. This is easy to implement but I am too afraid of losing the data on accident
- Create output options to SQL, CSV and mongodb

*I want to do all of these things but I only have so much time and frankly I need to focus on building my actual analysis work flow. The data is flowing right now without error so I will come back to this after I feel happy with the [daily option analysis](https://github.com/beachc15/daily-option-analysis) project (maybe never)*
