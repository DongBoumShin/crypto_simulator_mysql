import datetime
import time
import multiprocessing

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import essential_variable as ev
import pandas
import gc
import tqdm

import simul_methods as simul_methods
import algo_clean as algc
import algo_tester as algT
import globals

pymysql.install_as_MySQLdb()


class simulator:
    def __init__(self, stdate, days):
        self.atr = 2.9
        self.atr_stop = 0.8
        self.tempvar = 9
        self.stdate = stdate
        self.days = days

    def simulate(self, tup):
        gc.enable()
        mysqldb = "mysql+mysqldb://" + ev.db_id + ":" + ev.db_passwd + "@" + ev.db_ip + ":" + ev.db_port + "/" + "eth_data"
        simulatordb = "mysql+mysqldb://" + ev.db_id + ":" + ev.db_passwd + "@" + ev.db_ip + ":" + ev.db_port + "/" + "simulator0"
        engine_btc = create_engine(mysqldb)
        engine_sim = create_engine(simulatordb)
        simulMeth = simul_methods.simulMethods()
        with engine_btc.connect() as conn:
            simulMeth.fflush(conn)

            stmt = simulMeth.select_init_bulk(self.stdate, self.days)
            temp = simulMeth.create_init(conn, stmt)
            btc60min = temp[3].copy()
            btc15min = temp[1].copy()
            btc05min = temp[0].copy()
            btc30min = temp[2].copy()
            counters = [0, 0, 1]
            warningflag = [0, 0]
            algorithmres = []
            amount = 300000
            transactions = [[self.stdate, amount, 0, 0, 0, 0, 0, False]]
            hasFlag = False

            
            amalgam = tup[6]()
            amalgam.hourstop = 5
            amalgam.temp = self.tempvar
            #Assign to variables in algorithm from tuple

            firsttime = True
            # print(f"{tup} Start")
            while True:
                try:
                    btc05min = simulMeth.update_5min(btc05min, conn)
                    nope = 0
                    datemin = btc05min.iat[-1, 0].minute
                    update15 = [10, 25, 40, 55]
                    update30 = [25, 55]
                    update60 = [55]
                    if datemin in update60:
                        btc60min = simulMeth.update_60min(btc60min, conn)
                        counters[0] += 1
                        # counters[2] = 1

                    if datemin in update15:
                        btc15min = simulMeth.update_15min(btc15min, conn)
                        counters[1] += 1

                    if datemin in update30:
                        btc30min = simulMeth.update_30min(btc30min, conn)
                        pass

                    counters[2] += 1
                    # print(counters)
                    """if btc05min.iat[-1, 0] >= datetime.datetime(2021, 10, 29):
                        globals.TEMPANY = globals.CRITICALMULTIPLIER[1]
                    if btc05min.iat[-1, 0] >= datetime.datetime(2021, 11, 26):
                        globals.TEMPANY = globals.CRITICALMULTIPLIER[2]
                    if btc05min.iat[-1, 0] >= datetime.datetime(2021, 12, 24):
                        globals.TEMPANY = globals.CRITICALMULTIPLIER[3]
                    if btc05min.iat[-1, 0] >= datetime.datetime(2022, 1, 21):
                        globals.TEMPANY = globals.CRITICALMULTIPLIER[4]"""

                    nope = amalgam.buysell(algorithmres, warningflag, btc05min, btc15min, btc30min, btc60min, hasFlag)
                    if nope is None:
                        nope = 0
                    warningflag.append(nope)

                    if nope == 0:
                        pass
                    else:
                        if nope > 0 and not hasFlag:

                            if firsttime:
                                if nope == 1:
                                    transactions.append(simulMeth.longopen(transactions, btc05min))
                                    hasFlag = 1
                                elif nope == 2:
                                    transactions.append(simulMeth.shortopen(transactions, btc05min))
                                    hasFlag = 2
                                firsttime = False
                            else:
                                if nope == 1:
                                    transactions.append(simulMeth.longopen(transactions, btc05min))
                                    hasFlag = 1
                                elif nope == 2:
                                    transactions.append(simulMeth.shortopen(transactions, btc05min))
                                    hasFlag = 2
                        if (nope < 0) and hasFlag:
                            hasFlag = False
                            if nope == -1:
                                transactions.append(simulMeth.longclose(transactions, btc05min))
                            elif nope == -2:
                                transactions.append(simulMeth.shortclose(transactions, btc05min))
                            globals.TEMPSTOPLOSSDATA[-1][0] = transactions[-1][0]
                            if transactions[-1][4] < 0:

                                globals.TEMPSTOPLOSSDATA[-1][2] = -1
                            elif transactions[-1][4] > 0:

                                globals.TEMPSTOPLOSSDATA[-1][2] = 1

                    if btc05min.iat[-1, 0] >= self.stdate + self.days - datetime.timedelta(hours=2):
                        # 488, 132, 920, 384, 2780, 8340, 2648, 632, 5000, 10300, 26000
                        break
                except BaseException as BB:
                    raise BB

        if hasFlag == 1:
            transactions.append(simulMeth.longclose(transactions, btc05min))
        elif hasFlag == 2:
            transactions.append(simulMeth.shortclose(transactions, btc05min))
        globals.TEMPSTOPLOSSDATA[-1][0] = transactions[-1][0]
        if transactions[-1][4] < 0:
            globals.TEMPSTOPLOSSDATA[-1][2] = -1
        elif transactions[-1][4] > 0:
            globals.TEMPSTOPLOSSDATA[-1][2] = 1

        transactionsdf = pandas.DataFrame(transactions, columns=["index", "cash", "coin",
                                                                 "buyamountCash", "buycost", "sellcost",
                                                                 "pertransaction", "transactionOk"])
        algorithmres = pandas.DataFrame(algorithmres,
                                        columns=["dateindex", "stoploss", "target", 'price'])
        # (%, ATR), (0bXX, ATR)

        # globals.CRITICALMULTIPLIER.append([(self.sup_1, self.sup_2),
        #                                   round(-(1000000 - transactionsdf.iloc[-1, 1])/1000, 3)])
        with engine_sim.connect():
            # Change the dame of results
            """transactionsdf.to_sql(name=f'eth_fin-{self.stdate.month}_{self.stdate.day}-{tup[0]}-trans',
                                  con=engine_sim, if_exists='fail')
            algorithmres.to_sql(name=f'boxes{self.stdate.month}_{self.stdate.day}', con=engine_sim, if_exists='append')
            tempstoplossdata = pandas.DataFrame(globals.TEMPSTOPLOSSDATA,
                        columns=['Enddate', 'Startdate', 'WinLose', 'LongShort', 'Volume', 'VolPrice',
                        's22-65', 'volrsi', 'volmacd', 'voltoatr', 's22_65macd', 'price'])
            tempstoplossdata.to_sql(name=f'eth_fin-{self.stdate.month}_{self.stdate.day}-stats',
                                    con=engine_sim, if_exists='fail')"""

        tempup = np.where((transactionsdf['buycost'] > 0) & (transactionsdf['buycost'] < 100), 1, 0).sum()
        tempdown = np.where((transactionsdf['buycost'] < 0), 1, 0).sum()
        # print(btc05min.iat[-1, 0])
        engine_btc.dispose()
        engine_sim.dispose()
        return [tup, round(-(amount - transactionsdf.iloc[-1, 1]) / 1000, 3), tempup, tempdown,
                round(tempup / (tempup + tempdown) * 100, 2)]


if __name__ == "__main__":
    lst = [(i / 10, j/10, k, l / 10, m, n / 10, o) for i in [1] for j in [1]
           for k in [1] for l in [1] for m in [1] for n in [1]
           for o in [algc.AlgoSuperFin]]
    stdate = datetime.datetime(year=2022, month=4, day=1, hour=0, minute=0, second=0)  # 11/28~12/25, 10/31
    days = datetime.timedelta(days=30)  # 28, 53, 112

    simul = simulator(stdate, days)
    pool = multiprocessing.Pool(processes=6)
    outputs3 = []
    with tqdm.tqdm(total=len(lst)) as pbar:
        for x in tqdm.tqdm(pool.imap_unordered(func=simul.simulate, iterable=lst)):
            outputs3.append(x)
            pbar.update()
    sorted_by_second = sorted(outputs3, key=lambda tup: (tup[1], tup[4]), reverse=True)
    print(sorted_by_second)