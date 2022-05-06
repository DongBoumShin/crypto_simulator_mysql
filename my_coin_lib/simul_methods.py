from sqlalchemy import select
from sqlalchemy import update
import datetime
import pandas
import btc_tables as tables


# DataFrame is always
# ["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"]
# with integer indices: is not datetime index
import globals


class simulMethods:

    def __init__(self):
        self.min05 = None
        self.min15 = None
        self.min30 = None
        self.min60 = None

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

    def select_init_bulk(self, stdate, length: datetime.timedelta):
        stmt = [0, 1, 2, 3]
        enddate = stdate + length

        stmt[0] = select(tables.btcmin60).where(tables.btcmin60.c.index >= stdate -datetime.timedelta(hours=120)).where(
            tables.btcmin60.c.index <= enddate)
        stmt[1] = select(tables.btcmin15).where(tables.btcmin15.c.index >= stdate -datetime.timedelta(hours=30)).where(
            tables.btcmin15.c.index <= enddate)
        stmt[2] = select(tables.btcmin05).where(tables.btcmin05.c.index >= stdate -datetime.timedelta(hours=10)).where(
            tables.btcmin05.c.index <= enddate)
        stmt[3] = select(tables.btcmin30).where(tables.btcmin30.c.index >= stdate -datetime.timedelta(hours=60)).where(
            tables.btcmin30.c.index <= enddate)
        return stmt

    def create_init(self, connected, stmt, headlen=120):
        self.min60 = pandas.DataFrame(connected.execute(stmt[0]),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
        self.min15 = pandas.DataFrame(connected.execute(stmt[1]),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
        self.min05 = pandas.DataFrame(connected.execute(stmt[2]),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
        self.min30 = pandas.DataFrame(connected.execute(stmt[3]),
                                      columns=["Dateindex", "open", "high", "low", "close", "volume", "WhereAmI"])
        self.min05 = self.min05.drop_duplicates(subset=['Dateindex']).reset_index(drop=True)
        self.min15 = self.min15.drop_duplicates(subset=['Dateindex']).reset_index(drop=True)
        self.min30 = self.min30.drop_duplicates(subset=['Dateindex']).reset_index(drop=True)
        self.min60 = self.min60.drop_duplicates(subset=['Dateindex']).reset_index(drop=True)

        return [
            self.min05.head(headlen),
            self.min15.head(headlen),
            self.min30.head(headlen),
            self.min60.head(headlen)
        ]

    def update_5min(self, rows, connected):
        lastindex = list(rows.tail(1).index+1)[0]
        rows.drop(rows.head(1).index, inplace=True)
        try:
            deltime = datetime.timedelta(minutes=5)
            if self.min05.iat[lastindex, 0] > rows.iat[-1, 0] + deltime:
                temp = self.min05.loc[lastindex - 1].copy()
                temp['Dateindex'] = rows.iat[-1, 0] + deltime
                print(f"5분 데이터를 건너뜁니다, {temp['Dateindex']}")
                return rows.append(temp)
            return rows.append(self.min05.loc[lastindex])
        except KeyError:
            raise BaseException

    def update_15min(self, rows, connected):
        lastindex = rows.tail(1).index+1
        if lastindex > len(self.min15):
            return rows
        rows.drop(rows.head(1).index, inplace=True)
        try:
            return rows.append(self.min15.loc[lastindex])
        except KeyError:
            raise BaseException

    def update_60min(self, rows, connected):
        lastindex = list(rows.tail(1).index+1)[0]
        rows.drop(rows.head(1).index, inplace=True)
        deltime = datetime.timedelta(hours=1)
        try:
            if self.min60.iat[lastindex, 0] > rows.iat[-1, 0] + deltime:
                temp = self.min60.loc[lastindex - 1].copy()
                temp['Dateindex'] = rows.iat[-1, 0] + deltime
                print(f"60분 데이터를 건너뜁니다, {temp['Dateindex']}")
                return rows.append(temp)
            return rows.append(self.min60.loc[lastindex])
        except KeyError:
            raise BaseException

    def update_30min(self, rows, connected):
        lastindex = rows.tail(1).index+1
        if lastindex > len(self.min30):
            return rows
        rows.drop(rows.head(1).index, inplace=True)
        try:
            return rows.append(self.min30.loc[lastindex])
        except KeyError:
            raise BaseException

    def longopen(self, transactions, data05, amount=100000, fee=0.0005):
        coinamount = amount / data05.iloc[-1, 4]  # 4=close
        if transactions[-1][1] > amount:  # enough money
            transaction = [
                data05.iloc[-1, 0],  # datestanp
                transactions[-1][1] - amount * (1 + fee),  # cash balance
                transactions[-1][2] + coinamount,  # coin balance
                amount,  # how much bought
                data05.iloc[-1, 4],  # price of coin when buy
                (data05.iloc[-1, 5] - data05.iloc[-2, 5]) / data05.iloc[-2, 5],  # price of coin when sell
                -amount * (1 + fee),  # cash balance diff per transaction
                True  # TransactionOK
            ]
            #print("bought")
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
            #print("not bought")
        return transaction

    def longclose(self, transactions, data05, fee=0.0005):
        cashamount = globals.TEMPANY * transactions[-1][2] * (1 - fee)  # 4=close
        if transactions[-1][2] > 0:  # enough coins
            transaction = [
                data05.iloc[-1, 0],  # datestanp
                transactions[-1][1] + cashamount,  # cash balance
                0,  # coin balance, sell all
                -transactions[-1][2],  # how much bought
                (cashamount + transactions[-1][6]) / abs(transactions[-1][6]) * 100,  # percentage diff in %
                data05.iloc[-1, 4],  # price of coin when sell
                +cashamount,  # cash balance diff per transaction
                True  # TransactionOK
            ]
            #print("sold")
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
            #print("not sold")
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
            #print("shorted")
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
            #print("not shorted")
        return transaction

    def shortclose(self, transactions, data05, fee=0.0005):
        cashamount = globals.TEMPANY * transactions[-1][2] * (1 - fee)  # 4=close
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
            #print("short closed")
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
            #print("not closed")
        return transaction
