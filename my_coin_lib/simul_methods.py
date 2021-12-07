from sqlalchemy import select
from sqlalchemy import update
import datetime
import pandas
import my_coin_lib.btc_tables as tables

# DataFrame is always
# ["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"]
# with integer indices: is not datetime index


class simulMethods:

    def __init__(self):
        pass

    def fflush(self, connected):
        with connected.begin() as begin:
            stmat = update(tables.btcmin60).where(tables.btcmin60.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin30).where(tables.btcmin30.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin15).where(tables.btcmin15.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin05).where(tables.btcmin05.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            begin.commit()

    def select_init_bulk(self, stdate, length=200):
        self.length = length
        stmt = [0, 1, 2, 3]
        deltime0 = datetime.timedelta(minutes=60)
        deltime1 = datetime.timedelta(minutes=15)
        deltime2 = datetime.timedelta(minutes=5)
        deltime3 = datetime.timedelta(minutes=30)
        enddate0 = stdate - deltime0 * length
        enddate1 = stdate - deltime1 * length
        enddate2 = stdate - deltime2 * length
        enddate3 = stdate - deltime3 * length
        stmt[0] = select(tables.btcmin60).where(tables.btcmin60.c.index > enddate0).where(
            tables.btcmin60.c.index < stdate)
        stmt[1] = select(tables.btcmin15).where(tables.btcmin15.c.index > enddate1).where(
            tables.btcmin15.c.index < stdate)
        stmt[2] = select(tables.btcmin05).where(tables.btcmin05.c.index > enddate2).where(
            tables.btcmin05.c.index < stdate)
        stmt[3] = select(tables.btcmin30).where(tables.btcmin30.c.index > enddate3).where(
            tables.btcmin30.c.index < stdate)
        return stmt

    def create_init(self, connected, stmt):
        rows = pandas.DataFrame(connected.execute(stmt),
                                columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
        return rows

    def update_5min(self, rows, connected):
        currentdate = rows.iloc[len(rows.index)-1, 0]
        deltime2 = datetime.timedelta(minutes=5)
        currentdate = currentdate + deltime2
        with connected.begin() as begin:
            stmat = update(tables.btcmin05).where(tables.btcmin05.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin05).where(tables.btcmin05.c.index == currentdate).values(WhereAmI=1)
            connected.execute(stmat)
            begin.commit()

        stmat = select(tables.btcmin05).where(tables.btcmin05.c.WhereAmI == 1)

        try:
            newrow = pandas.DataFrame(connected.execute(stmat),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
            if len(newrow.index) == 0:
                currentdate += deltime2*self.length
                rows = self.select_init_bulk(currentdate, self.length)
                rows = self.create_init(connected, rows[2])
                print("5분 데이터를 건너뜁니다.")
                if len(rows) < self.length/2:
                    raise BaseException
                return rows
        except BaseException as BB:
            print("5분 데이터베이스를 전부 돌았습니다.", BB)
            raise
        rows = pandas.concat([rows, newrow], ignore_index=True)
        rows.drop(0, inplace=True)
        rows.reset_index(inplace=True)
        rows.drop("index", axis=1, inplace=True)
        return rows

    def update_15min(self, rows, connected):
        currentdate = rows.iloc[len(rows.index)-1].Dateindex
        deltime2 = datetime.timedelta(minutes=15)
        currentdate = currentdate + deltime2
        with connected.begin() as begin:
            stmat = update(tables.btcmin15).where(tables.btcmin15.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin15).where(tables.btcmin15.c.index == currentdate).values(WhereAmI=1)
            connected.execute(stmat)
            begin.commit()

        stmat = select(tables.btcmin15).where(tables.btcmin15.c.WhereAmI == 1)
        try:
            newrow = pandas.DataFrame(connected.execute(stmat),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
            if len(newrow.index) == 0:
                currentdate += deltime2 * self.length
                rows = self.select_init_bulk(currentdate, self.length)
                rows = self.create_init(connected, rows[1])
                print("15분 데이터를 건너뜁니다.")
                if len(rows) < self.length / 2:
                    raise BaseException
                return rows
        except BaseException as BB:
            print("15분 데이터베이스를 전부 돌았습니다.", BB)
            raise
        rows = pandas.concat([rows, newrow], ignore_index=True)
        rows.drop(0, inplace=True)
        rows.reset_index(inplace=True)
        rows.drop("index", axis=1, inplace=True)
        return rows

    def update_60min(self, rows, connected):
        currentdate = rows.iloc[len(rows.index)-1].Dateindex
        deltime2 = datetime.timedelta(minutes=60)
        currentdate = currentdate + deltime2
        with connected.begin() as begin:
            stmat = update(tables.btcmin60).where(tables.btcmin60.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin60).where(tables.btcmin60.c.index == currentdate).values(WhereAmI=1)
            connected.execute(stmat)
            begin.commit()

        stmat = select(tables.btcmin60).where(tables.btcmin60.c.WhereAmI == 1)
        try:
            newrow = pandas.DataFrame(connected.execute(stmat),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
            if len(newrow.index) == 0:
                currentdate += deltime2 * self.length
                rows = self.select_init_bulk(currentdate, self.length)
                rows = self.create_init(connected, rows[0])
                print("60분 데이터를 건너뜁니다.")
                if len(rows) < self.length / 2:
                    raise BaseException
                return rows
        except BaseException as BB:
            print("60분 데이터베이스를 전부 돌았습니다.", BB)
            raise
        rows = pandas.concat([rows, newrow], ignore_index=True)
        rows.drop(0, inplace=True)
        rows.reset_index(inplace=True)
        rows.drop("index", axis=1, inplace=True)
        return rows

    def update_30min(self, rows, connected):
        currentdate = rows.iloc[len(rows.index)-1].Dateindex
        deltime2 = datetime.timedelta(minutes=30)
        currentdate = currentdate + deltime2
        with connected.begin() as begin:
            stmat = update(tables.btcmin30).where(tables.btcmin30.c.WhereAmI == 1).values(WhereAmI=0)
            connected.execute(stmat)
            stmat = update(tables.btcmin30).where(tables.btcmin30.c.index == currentdate).values(WhereAmI=1)
            connected.execute(stmat)
            begin.commit()

        stmat = select(tables.btcmin30).where(tables.btcmin30.c.WhereAmI == 1)
        try:
            newrow = pandas.DataFrame(connected.execute(stmat),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
            if len(newrow.index) == 0:
                currentdate += deltime2 * self.length
                rows = self.select_init_bulk(currentdate, self.length)
                rows = self.create_init(connected, rows[2])
                print("30분 데이터를 건너뜁니다.")
                if len(rows) < self.length / 2:
                    raise BaseException
                return rows
        except BaseException as BB:
            print("30분 데이터베이스를 전부 돌았습니다.", BB)
            raise
        rows = pandas.concat([rows, newrow], ignore_index=True)
        rows.drop(0, inplace=True)
        rows.reset_index(inplace=True)
        rows.drop("index", axis=1, inplace=True)
        return rows

    def longopen(self, transactions, data05, amount=100000, fee=0.0005):
        coinamount = amount/data05.iloc[-1, 4]#4=close
        if transactions[-1][1] > amount:#enough money
            transaction = [
                data05.iloc[-1, 0],#datestanp
                transactions[-1][1]-amount*(1+fee),#cash balance
                transactions[-1][2]+coinamount,#coin balance
                amount,#how much bought
                data05.iloc[-1, 4],#price of coin when buy
                (data05.iloc[-1, 5]-data05.iloc[-2, 5])/data05.iloc[-2, 5],#price of coin when sell
                -amount*(1+fee),#cash balance diff per transaction
                True#TransactionOK
            ]
            print("bought")
        else:#not enough money
            transaction = [
                data05.iloc[-1, 0],#datestanp
                transactions[-1][1],#cash balance
                transactions[-1][2],#coin balance
                0,#how much bought
                data05.iloc[-1, 4],#price of coin when buy
                0,#price of coin when sell
                0,#cash balance diff per transaction
                False#TransactionOK
            ]
            print("not bought")
        return transaction

    def longclose(self, transactions, data05, fee=0.0005):
        cashamount = data05.iloc[-1, 4]*transactions[-1][2]*(1-fee)#4=close
        if transactions[-1][2] > 0:#enough coins
            transaction = [
                data05.iloc[-1, 0],#datestanp
                transactions[-1][1]+cashamount,#cash balance
                0,#coin balance, sell all
                -transactions[-1][2],#how much bought
                (cashamount+transactions[-1][6])/abs(transactions[-1][6])*100,#percentage diff in %
                data05.iloc[-1, 4],#price of coin when sell
                +cashamount,#cash balance diff per transaction
                True#TransactionOK
            ]
            print("sold")
        else:#not enough coins
            transaction = [
                data05.iloc[-1, 0],#datestanp
                transactions[-1][1],#cash balance
                transactions[-1][2],#coin balance
                0,#how much bought
                0,#price of coin when buy
                data05.iloc[-1, 4],#price of coin when sell
                0,#cash balance diff per transaction
                False#TransactionOK
            ]
            print("not sold")
        return transaction

    def shortopen(self, transactions, data05, amount=100000, fee=0.0005):
        coinamount = amount / data05.iloc[-1, 4]  # 4=close
        if transactions[-1][1] > amount:  # enough money
            transaction = [
                data05.iloc[-1, 0],  # datestanp
                transactions[-1][1] + amount * (1 - fee),  # cash balance
                transactions[-1][2] - coinamount,  # coin balance
                amount,  # how much shorted
                data05.iloc[-1, 4],  # price of coin when short
                (data05.iloc[-1, 5] - data05.iloc[-2, 5]) / data05.iloc[-2, 5],  # price of coin when sell
                amount * (1 - fee),  # cash balance diff per transaction
                True  # TransactionOK
            ]
            print("shorted")
        else:  # not enough money
            transaction = [
                data05.iloc[-1, 0],  # datestanp
                transactions[-1][1],  # cash balance
                transactions[-1][2],  # coin balance
                0,  # how much bought
                data05.iloc[-1, 4],  # price of coin when buy
                0,  # price of coin when sell
                0,  # cash balance diff per transaction
                False  # TransactionOK
            ]
            print("not shorted")
        return transaction

    def shortclose(self, transactions, data05, fee=0.0005):
        cashamount = data05.iloc[-1, 4] * transactions[-1][2] * (1 - fee)  # 4=close
        if transactions[-1][2] < 0:  # enough coins
            transaction = [
                data05.iloc[-1, 0],  # datestanp
                transactions[-1][1] + cashamount,  # cash balance
                0,  # coin balance, sell all
                -transactions[-1][2],  # how much bought
                (cashamount + transactions[-1][6]) / transactions[-1][6] * 100,  # percentage diff in %
                data05.iloc[-1, 4],  # price of coin when sell
                cashamount,  # cash balance diff per transaction
                True  # TransactionOK
            ]
            print("short closed")
        else:  # not enough coins
            transaction = [
                data05.iloc[-1, 0],  # datestanp
                transactions[-1][1],  # cash balance
                transactions[-1][2],  # coin balance
                0,  # how much bought
                0,  # price of coin when buy
                data05.iloc[-1, 4],  # price of coin when sell
                0,  # cash balance diff per transaction
                False  # TransactionOK
            ]
            print("not closed")
        return transaction
