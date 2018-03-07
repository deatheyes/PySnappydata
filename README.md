PySanppyData
===

PySnappyData is a collection of Python [DB-API](http://www.python.org/dev/peps/pep-0249/) and [SQLAlchemy](http://www.sqlalchemy.org/) interfaces for [SanppyData](https://www.snappydata.io/)

# Usage

## DB-API

``` python
from pysnappydata import snappydata
cursor = snappydata.connect('localhost').cursor()
cursor.execute('SELECT * FROM test')
print cursor.fetchall()
```

## SQLAlchemy

First install SQLAlchemy, then install this package to register it with SQLAlchemy:

### Work with server connection

``` python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('snappydata://localhost')
metadata = MetaData()
user = Table('user_c', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(20)),
    )
metadata.create_all(engine)
conn = engine.connect()

i = user.insert()
u = dict(id=0, name='yy')
r = conn.execute(i, **u)
print r.rowcount

s = select([user.c.name]).where(user.c.id==0)
r = conn.execute(s)
print r.rowcount
print r.fetchall()
```
### Work with locator connection

``` python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('snappydata://localhost:1527', connect_args = {'locator':True})
    metadata = MetaData()
    user = Table('user_c', metadata,
             Column('id', INTEGER, primary_key=True),
             Column('name', VARCHAR(20)),
             )
    metadata.create_all(engine)
    conn = engine.connect()
    user.drop(engine);
    conn.close()
```
