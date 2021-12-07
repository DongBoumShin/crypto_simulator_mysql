import numpy as np
import pandas


# {"macd":macd, "macdsig":macdsig, "macdosc": macdosc}
# {"K":slowk, "D":slowd, "S": slows, "osci": osci}
def sma(df, period):
    smares = df['close'].rolling(window=period).mean()
    smares.name = 'sma' + str(period)
    return smares


def ema(df, signal):  # returns series
    emares = df['close'].ewm(span=signal).mean()
    return emares


def heikin_ashi(df):  # returns dataframe
    # heikin close
    heikin = df.assign(close=lambda x: round((x['open'] + x['high'] + x['low'] + x['close']) / 4, 2))
    # heikin open
    for i in heikin.index:
        if i == 0:
            heikin.iat[0, 1] = round((df.iat[0, 1] + df.iat[0, 4]) / 2, 2)
        else:
            heikin.iat[i, 1] = round((heikin.iat[i - 1, 1] + heikin.iat[i - 1, 4]) / 2, 2)
    # heikin high
    heikin['high'] = heikin.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
    # heikin low
    heikin['low'] = heikin.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
    return heikin


def calcRSI(df, period=14):  # returns one column dataframe
    date_index = df.index
    U = np.where(df.diff(periods=1)['close'] > 0, df.diff(periods=1)['close'], 0)
    D = np.where(df.diff(periods=1)['close'] < 0, df.diff(periods=1)['close'] * (-1), 0)
    AU = pandas.DataFrame(U, index=date_index).rolling(window=period).mean()
    AD = pandas.DataFrame(D, index=date_index).rolling(window=period).mean()
    RSI = AU / (AU + AD) * 100
    return RSI


def rsiEMA(df, signal=9):  # returns series
    df = pandas.DataFrame(df)
    ema = df.RSI.ewm(span=signal).mean()
    return ema


def divergence(df, series, length=20):
    taildf = df.tail(length)
    tailse = pandas.DataFrame(series.tail(length))
    div = 0
    lowsdf = taildf['low'].nsmallest(2).index.values.tolist()
    highdf = taildf['high'].nlargest(2).index.values.tolist()
    lowsse = tailse.nsmallest(2, 0).index.values.tolist()
    highse = tailse.nlargest(2, 0).index.values.tolist()
    if lowsdf[0] > lowsdf[1]:
        # lower low df
        if lowsse[0] < lowsse[1]:
            div = 1  # higher low
    if highdf[0] > highdf[1]:
        # higher high
        if highse[0] < highse[1]:
            div = -1  # lower high
    return div


def macd(df, short=13, long=26, signal=9):  # returns dataframe
    df = pandas.DataFrame(df)

    ema_s = df.close.ewm(span=short).mean()
    ema_l = df.close.ewm(span=long).mean()
    macd = ema_s - ema_l
    macdsig = macd.ewm(span=signal).mean()
    macdosc = macd - macdsig

    frame = {"macd": macd, "macdsig": macdsig, "macdosc": macdosc}
    macd_data = pandas.DataFrame(frame)

    return macd_data


def macdoscEMA(df, signal=3):  # returns series
    df = pandas.DataFrame(df)
    ema = df.macdosc.ewm(span=signal).mean()
    return ema


# Fast %K = ((현재가 - n기간 중 최저가) / (n기간 중 최고가 - n기간 중 최저가)) * 100
# close_price = ohlcv["close"], low = ohlcv["low"], high = ohlcv["high"]
def get_stochastic_fast_k(close_price, low, high, n=9):  # returns series
    fast_k = ((close_price - low.rolling(n).min()) / (high.rolling(n).max() - low.rolling(n).min())) * 100
    return fast_k


# Slow %K = Fast %K의 m기간 이동평균(SMA)
def get_stochastic_slow_k(fast_k, n=4):  # returns series
    slow_k = fast_k.rolling(n).mean()
    return slow_k


# Slow %D = Slow %K의 t기간 이동평균(SMA)
def get_stochastic_slow_d(slow_k, n=3):  # returns series
    slow_d = slow_k.rolling(n).mean()
    return slow_d


def get_stochastic(df, n=9, k=4, d=3):
    fast = get_stochastic_fast_k(df.close, df.low, df.high, n)
    slowk = get_stochastic_slow_k(fast, k)
    slowd = get_stochastic_slow_d(slowk, d)
    slows = (slowd + slowk) / 2
    osci = slowk - slowd
    frame = {"K": slowk, "D": slowd, "S": slows, "osci": osci}
    stochastic = pandas.DataFrame(frame)
    return stochastic


def macd_average_slope(macddata):  # returns series
    hly = macddata.apply(lambda x: (x['macd'] + x['macdsig']) / 2, axis=1)
    hly2 = pandas.DataFrame(hly.diff(periods=1))
    return hly2


def macd_oscema_slope(macdema):
    hly2 = pandas.DataFrame(macdema.diff(periods=1))
    return hly2


def atr(df, period=14):  # returns series
    true_range = pandas.DataFrame()
    true_range['high_low'] = df['high'] - df['low']
    true_range['high_close'] = np.abs(df['high'] - df['close'].shift(1))
    true_range['low_close'] = np.abs(df['low'] - df['close'].shift(1))
    atrnow = true_range.max(axis=1).ewm(period).mean()
    return atrnow


def adx(df, period=14):
    direction = pandas.DataFrame()
    direction['atr'] = atr(df, period)
    direction['dmup'] = np.where(df['high'].diff(1) > 0, df['high'].diff(1), 0)
    direction['dmdown'] = np.where(df['low'].diff(1) < 0, df['low'].diff(1), 0)
    direction['diup'] = direction['dmup'].ewm(span=period).mean() * 100 / direction['atr']
    direction['didown'] = abs(direction['dmdown'].ewm(span=period).mean() * 100 / direction['atr'])
    direction['dx'] = abs((direction['diup'] - direction['didown']) * 100 / (direction['diup'] + direction['didown']))
    adxnow = pandas.DataFrame()
    adxnow['adx'] = (direction['dx'].shift(1) * (period - 1) + direction['dx']) / period
    adxnow['diup'] = direction['diup']
    adxnow['didown'] = direction['didown']
    return adxnow


def mvawp(df, period=50):  # returns np array
    dftemp = df.tail(period)
    v = dftemp['volume'].values
    h = dftemp.high.values
    l = dftemp.low.values
    c = dftemp.close.values
    return pandas.Series(np.cumsum(v * (h + l + c) / 3) / np.cumsum(v))


def mvawp1(df, period):
    v = df['volume'].rolling(period).sum()
    h = df['high'].rolling(period).sum()
    l = df['low'].rolling(period).sum()
    c = df['close'].rolling(period).sum()
    return (v * (h + l + c) / 3) / (v)


def fractal(df, period=3):
    if period % 2 == 0 or period < 2:
        raise ValueError
    tempdf = df.tail(period)
    focus = int((period-1)/2)
    if tempdf.iat[focus, 2] >= tempdf['high'].max():
        return 2
    if tempdf.iat[focus, 3] <= tempdf['low'].min():
        return 1


def supertrend(df, period=10, mul=3):
    taillen = 50
    atrtemp = atr(df, period)
    hla = (df['high'] + df['low']) / 2
    superdata = [True] * taillen #len(df.index)
    basic_upper = hla + mul * atrtemp
    basic_lower = hla - mul * atrtemp
    df = df.tail(taillen).copy().reset_index().drop('index', axis=1)
    basic_lower = basic_lower.tail(taillen).copy().reset_index().drop('index', axis=1).values.tolist()
    basic_upper = basic_upper.tail(taillen).copy().reset_index().drop('index', axis=1).values.tolist()
    # FINAL UPPERBAND = IF( (Current BASICUPPERBAND  < Previous FINAL UPPERBAND
    # or Previous FINAL UPPERBAND < Previous Close )
    # THEN (Current BASIC UPPERBAND) ELSE Previous FINALUPPERBAND)

    # FINAL LOWERBAND = IF( (Current BASIC LOWERBAND > Previous FINAL LOWERBAND
    # or Previous FINAL LOWERBAND > Previous Close)
    # THEN (Current BASIC LOWERBAND) ELSE Previous FINAL LOWERBAND)

    # Supertrend
    # if prev.Supertrend == prev.Upper
    #   curr.close < curr.Upper: curr.Supertrend = curr.Upper
    #   else: curr.Supertrend = curr.Lower
    # if prev.Supertrend == prev.Lower
    #    curr.close < curr.Lower: curr.Supertrend = curr.Upper
    #    else: curr.Supertrend = curr.Lower

    for i in range(1, len(df.index)):
        curr, prev = i, i - 1
        # adjustment to the final bands
        if not (df.iat[prev, 4] < basic_lower[prev][0] or basic_lower[prev][0] < basic_lower[curr][0]):
            basic_lower[curr][0] = basic_lower[prev][0]
        if not (df.iat[prev, 4] > basic_upper[prev][0] or basic_upper[prev][0] > basic_upper[curr][0]):
            basic_upper[curr][0] = basic_upper[prev][0]
        # False = Upper = Short
        # True = Lower = Long
        if superdata[prev] is True:
            if df.iat[curr, 4] <= basic_lower[curr][0]:
                superdata[curr] = False
        else:
            if df.iat[curr, 4] <= basic_upper[curr][0]:
                superdata[curr] = False

    return pandas.DataFrame({
        'supertrend': superdata,
        'lower': basic_lower,
        'upper': basic_upper
    })



