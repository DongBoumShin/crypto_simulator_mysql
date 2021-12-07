# crypto_simulator_mysql
simple simulator for crypto algorithms fetching from local mysql db

CryptoCurrency investing is more or less a gamble, and large sums of capital will be lost. None of the below, and any results you get from using this simulator are in any form reliable financial advisements.

Use at your own risk, and I am not liable for any results this code might bring.

Although if you do have any success, could you kindly share your secrets with me?


This simulator takes one single asset's data from a local mysql database and simulates on a given timeframe. Does not support multi-asset simulations.

This simulator also does not support opening multiple positions before closing. That is, one can only have one single position open, whether it be long or short, at any given time. I myself did not need this function, so if you need it, please feel free to implement yourself.

The simulator fetches candlestick data, i.e. datetime, open, high, low, close, volume data as a dataframe of a given length, default 90. It currently fetches 4 timeframes: 5min, 15min, 30min, 60min. may be able to support 1min and 240min if you tweak the tables and update function accordingly.

The simulator is slow, as it fetches row by row from the db, but the slow mechanism is a side effect resulting from avoiding any look-ahead bias that might form.


The Simulator

The most important thing to note when using this simulator: It does not take into account unclosed candles.

That is, any 'current'candle on higher timeframes are dropped until the candles are closed.
For example: if the clock is pointing 01:12, the last candle on the 5min is 01:05~10, 15min: 00:45~01:00, 30min: 00:30~01:00, 60min: 00:00~01:00.
As such, 60min data isn't updated until the clock hits 00, when the hourly candle is finally closed.

Because there was no need for me to package it into a standalone program, variables are mostly in the source code directly.

There are 6 main areas you might want to change according to your specifications: 

Database name of the Asset,
Database name of the results, 
Start date,
Algorithm,
Duration,
Name of the resulting tables.

DB names are stored in variables named 'mysqldb' and 'simulatordb'. I was using ETH for simulations, so 'eth_data' and 'simulator0'

Start date is in datetime.datetime and stored in a variable named 'stdate'. Specify the year, month, date and minute accordingly.

Algorithm is an object of an Algorithm class, which I will specify below. It is stored in a variable named 'amalgam'.

Duration is a positive raw integer, which is compared to 'counter[1]'. counter[1] counts how many 15 min candles have passed since start date. 2780 indicates about a month's time, 488 about a week and so on.

Result tables are stored in variables named 'transactions' and 'algorithmres', which are transported to the database named above. Please change the names of the resulting tables after each run, as the simulator is set to fail if the name exists already.


The Algorithm

Any algorithm is to be stored under the file 'algo_clean'. Should the file overfill, feel free to create another file and import it to simulator, as long as it inherits class Algorithm or AlgorithmTrailStop class from algo_clean.

An Algorithm has 1 method: buysell. 'buysell' method is expected to return a flag of integers: -1, -2, 1, 2, 0, where:
1 means to open a long position, -1 to close a long position,
2 means to open a short position, -2 to close a short position,
and 0 to do nothing. Any other integer or NONE should do nothing, although isn't guaranteed

'buysell' accepts 2 empty variables: algorithmres: a list of lists, and warningflag, a list, purely for tracking whatever you need to. Variable algorithmres is exported to a database table at the end of each run while warningflag is not.
'buysell' also accepts 5 dataframes for 5min, 15min, 30min, 60min and a boolean flag for whether a position is open or not.

AlgorithmTraistop contains a few more methods. For setting trailing stops, the target and the stop is arbitrary and can be adjusted in methods '_set_trade_area_' and '_update_trade_area_'. I was using a combination of an arbitrary multiplier, atr, and the current low/high.

Also, AlgorithmTrailStop's 'buysell' method is cluttered with necessary methods to make a trailing stop, so '_buysell' method is used to return necessary integer flags -2~2.

An example algorithm of AlgorithmTrailStop is included, although it will return horrible results.


Simulator methods

The simulator expects the Database of the asset to be in the following format of a pandas DataFrame:
"Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"

"WhereAmI" is just used to track where in the database the current and next rows are.

This is where long/short open and close are. The method is straightforward enough, and instuctions are in inline comments.


Globals & Essential Variables

If you need more than 2 slots for tracking anything, please feel free to use 'globals' variables. I currently am using a global variable "TEMPSTOPLOSSDATA" to track stoploss & target for trailing stops. It's a messy fix, but it works.

Essential_variables file only contains db_id, db_password, db_ip and db_port. I have mine on localhost, but if you don't, change them here.


Important Methods

Please use Pandas-TA, technical analysis, but when I first started out, I did not know about them, so I wrote & copied important technical indicators from the web. They all work as intended, although the divergence method is untested, and none are optimised. Instructions are in comments/inline comments for each method.

PreReqs

You need Pandas, Numpy, PyMySQL, SQLAlchemy.

I've been using this for about a year now, and there shouldn't be too many bugs. Should any bug occur, please comment.
