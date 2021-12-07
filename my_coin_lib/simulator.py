import datetime
from sqlalchemy import create_engine
import pymysql
import essential_variable as ev
import pandas
import gc

import simul_methods as simul_methods
import algo_clean as algc
import globals

pymysql.install_as_MySQLdb()


class simulator:
    def __init__(self):
        pass

    def simulate(self):
        gc.enable()
        mysqldb = "mysql+mysqldb://" + ev.db_id + ":" + ev.db_passwd + "@" + ev.db_ip + ":" + ev.db_port + "/" + "eth_data"
        simulatordb = "mysql+mysqldb://" + ev.db_id + ":" + ev.db_passwd + "@" + ev.db_ip + ":" + ev.db_port + "/" + "simulator0"
        engine_btc = create_engine(mysqldb)
        engine_sim = create_engine(simulatordb)
        simulMeth = simul_methods.simulMethods()
        with engine_btc.connect() as conn:
            simulMeth.fflush(conn)
            stdate = datetime.datetime(year=2021, month=7, day=2, hour=0, minute=0, second=0)
            stmt = simulMeth.select_init_bulk(stdate)
            btc60min = simulMeth.create_init(conn, stmt[0])
            btc15min = simulMeth.create_init(conn, stmt[1])
            btc05min = simulMeth.create_init(conn, stmt[2])
            btc30min = simulMeth.create_init(conn, stmt[3])
            counters = [0, 0, 1]
            warningflag = [0, 0]
            algorithmres = [[0,0,0,0,0,0,0,0,0,0,0]]
            transactions = [[stdate, 100000000, 0, 0, 0, 0, 0, False]]
            hasFlag = False

            amalgam = algc.AlgoSuperAlt1()

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
                        counters[2] = 1

                    if datemin in update15:
                        btc15min = simulMeth.update_15min(btc15min, conn)
                        counters[1] += 1

                    if datemin in update30:
                        btc30min = simulMeth.update_30min(btc30min, conn)

                    counters[2] += 1
                    print(counters)

                    nope = amalgam.buysell(algorithmres, warningflag, btc05min, btc15min, btc30min, btc60min, hasFlag)
                    if nope is None:
                        nope = 0
                    warningflag.append(nope)
                    if nope == 0:
                        pass
                    else:
                        if nope > 0 and not hasFlag:
                            if nope == 1:
                                transactions.append(simulMeth.longopen(transactions, btc05min))
                                hasFlag = 1
                            elif nope == 2:
                                transactions.append(simulMeth.shortopen(transactions, btc05min))
                                hasFlag = 2
                        if nope < 0 and hasFlag:
                            hasFlag = False
                            if nope == -1:
                                transactions.append(simulMeth.longclose(transactions, btc05min))
                            elif nope == -2:
                                transactions.append(simulMeth.shortclose(transactions, btc05min))

                    if counters[1] >= 2780:  # 488, 132, 960, 384, 2780, 8340
                        transactionsdf = pandas.DataFrame(transactions, columns=["index", "cash", "coin",
                                                                                 "buyamountCash", "buycost", "sellcost",
                                                                                 "pertransaction", "transactionOk"])
                        algorithmres = pandas.DataFrame(algorithmres,
                                                        columns=["dateindex", "now", "back1", "back2", "back3", "back4",
                                                                 "back5", "back6", "back7", "back8", "back9"])
                                                        # (%, ATR), (0bXX, ATR)
                        with engine_sim.connect():
                            # Change the dame of results
                            transactionsdf.to_sql(name='eth_super-0702-5-trans', con=engine_sim, if_exists='fail')
                            algorithmres.to_sql(name='__', con=engine_sim, if_exists='append')
                            #tempstoplossdata = pandas.DataFrame(globals.TEMPSTOPLOSSDATA,
                            #                                    columns=['dateindex', 'stoploss', 'target', 'price'])
                            #tempstoplossdata.to_sql(name='eth_super-0702-5-stoploss', con=engine_sim, if_exists='fail')
                        for i in range(len(warningflag)):
                            if warningflag[i] != 0:
                                print(i)
                                print(warningflag[i])
                        break
                except BaseException as BB:
                    transactionsdf = pandas.DataFrame(transactions, columns=["index", "cash", "coin",
                                                                             "buyamountCash", "buycost", "sellcost",
                                                                             "pertransaction", "transactionOk"])
                    transactionsdf.to_sql(name='temp', con=engine_btc, if_exists='append')
                    algorithmres = pandas.DataFrame(algorithmres, columns=["date", "updown05", "updown15st", "updown15",
                                                                           "updown60slope", "updown60"])
                    algorithmres.to_sql(name='simul-b-1', con=engine_btc, if_exists='fail')
                    print(BB)
                    for i in warningflag:
                        if i != 0:
                            print(i)
                    break


if __name__ == "__main__":
    globals.initialize_globals()
    simul = simulator()
    simul.simulate()
