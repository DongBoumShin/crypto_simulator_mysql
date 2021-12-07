import pandas
import numpy as np
import my_coin_lib.important_methods as meth
import datetime


class Algorithm:
    def __init__(self):
        pass

    def buysell(self, algorithmres, warningflag, data05, data15, data30, data60, hasbool):
        # expects to return 1, 2, -1, -2, 0
        pass


class AlgorithmTrailStop(Algorithm):
    def __init__(self):
        super().__init__()
        self.data05 = None
        self.curprice = None
        self.avprice = None
        self.has = None
        self.stoploss = None
        self.target = None
        self.atr_var = None

    def buysell(self, algorithmres, warningflag, data05, data15, data30, data60, hasbool):
        self.data05 = data05
        self.data15 = data15
        self.data30 = data30
        self.data60 = data60
        self.has = hasbool
        self.curprice = data05.iat[-1, 4]
        losscut = self.sellcond()
        if losscut:
            return losscut
        flag = self._buysell()
        if not flag:
            flag = 0
        if not self.has:
            if flag == 1:
                self._set_trade_area_long()
                globals.TEMPSTOPLOSSDATA.append([self.data05.iat[-1, 0], self.stoploss, self.target, self.curprice])
            elif flag == 2:
                self._set_trade_area_short()
                globals.TEMPSTOPLOSSDATA.append([self.data05.iat[-1, 0], self.stoploss, self.target, self.curprice])
        return flag

    def sellcond(self):
        if self.has == 1:
            if self.curprice > self.target:
                print("won-l")
                self._update_trade_area_long()
            if self.curprice < self.stoploss:
                print("stoploss_l")
                globals.TEMPSTOPLOSSDATA.append([self.data05.iat[-1, 0], np.NaN, np.NaN, self.curprice])
                return -1
            #if self.curprice < self.avprice * 0.99:
            #    print('-1%')
            #    return -1
        elif self.has == 2:
            if self.curprice < self.target:
                print("won-sh")
                self._update_trade_area_short()
            if self.curprice > self.stoploss:
                print("stoploss_sh")
                globals.TEMPSTOPLOSSDATA.append([self.data05.iat[-1, 0], np.NaN, np.NaN, self.curprice])
                return -2
            #if self.curprice > self.avprice * 1.01:
            #    print('-1%_sh')
            #    return -2

    def _set_trade_area_short(self):
        target = meth.atr(self.data05).iat[-1]  # + (self.curprice - self.data05.iat[-1, 3])
        mul = 1
        if target <= self.curprice * 0.01:
            mul = 1.5
            # mul = round(self.curprice * 0.01 / target, 2)
        self.target = self.curprice - 2.8 * mul * target
        self.stoploss = self.data05.iat[-1, 2] + 1 * mul * target
        self.avprice = self.curprice
        self.atr_var = target * mul


    def _set_trade_area_long(self):
        target = meth.atr(self.data05).iat[-1]  # + (self.curprice - self.data05.iat[-1, 3])
        mul = 1
        if target <= self.curprice * 0.01:
            mul = 1.5
            # mul = round(self.curprice * 0.01 / target, 2)
        self.target = self.curprice + 2.8 * mul * target
        self.stoploss = self.data05.iat[-1, 2] - 1 * mul * target
        self.avprice = self.curprice
        self.atr_var = target * mul

    def _update_trade_area_short(self):
        self.target = self.curprice - 1.9 * self.atr_var
        self.stoploss = self.curprice + 0.5 * self.atr_var
        globals.TEMPSTOPLOSSDATA.append([self.data05.iat[-1, 0], self.stoploss, self.target, self.curprice])

    def _update_trade_area_long(self):
        self.target = self.curprice + 1.9 * self.atr_var
        self.stoploss = self.curprice - 0.5 * self.atr_var
        globals.TEMPSTOPLOSSDATA.append([self.data05.iat[-1, 0], self.stoploss, self.target, self.curprice])

    def _buysell(self) -> int:
        # expects to return 1, 2, -1, -2, 0
        pass


class ExampleAlgorithm(AlgorithmTrailStop):
    def __init__(self):
        super().__init__()

    def _buysell(self) -> int:
        sma05 = meth.sma(self.data05, 5)
        sma15 = meth.ema(self.data15, 15)
        supertrend = meth.supertrend(self.data05)
        if supertrend['supertrend'].iat[-1]:
            if self.curprice >= sma05.iat[-1] >= sma15.iat[-1]:
                return 1
        elif not supertrend['supertrend'].iat[-1]:
            if self.curprice <= sma05.iat[-1] <= sma15.iat[-1]:
                return 2

