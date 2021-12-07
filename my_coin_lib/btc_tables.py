from sqlalchemy import MetaData, Table, Column, DateTime, Float, Integer, Boolean

metadata = MetaData()

transactions = Table('simul1', metadata,
                     Column('index', DateTime, primary_key=True, nullable=False),
                     Column('cash', Float),
                     Column('coin', Float),
                     Column('buyamountCash', Float),
                     Column('buycost', Float),
                     Column('sellcost', Float),
                     Column('pertransaction', Float),
                     Column('transactionOk', Boolean),
                     )

btcmin60 = Table('min60', metadata,
                 Column('index', DateTime, primary_key=True, nullable=False),
                 Column('Open', Float),
                 Column('High', Float),
                 Column('Low', Float),
                 Column('Close', Float),
                 Column('Volume', Float),
                 Column('WhereAmI', Integer),)

btcmin30 = Table('min30', metadata,
                 Column('index', DateTime, primary_key=True, nullable=False),
                 Column('Open', Float),
                 Column('High', Float),
                 Column('Low', Float),
                 Column('Close', Float),
                 Column('Volume', Float),
                 Column('WhereAmI', Integer),)

btcmin15 = Table('min15', metadata,
                 Column('index', DateTime, primary_key=True, nullable=False),
                 Column('Open', Float),
                 Column('High', Float),
                 Column('Low', Float),
                 Column('Close', Float),
                 Column('Volume', Float),
                 Column('WhereAmI', Integer),)

btcmin05 = Table('min05', metadata,
                 Column('index', DateTime, primary_key=True, nullable=False),
                 Column('Open', Float),
                 Column('High', Float),
                 Column('Low', Float),
                 Column('Close', Float),
                 Column('Volume', Float),
                 Column('WhereAmI', Integer),)
