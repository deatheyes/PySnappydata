PySnappyData
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

## Superset

With SQLAlchemy supports, we can benefit from [Superset](https://github.com/apache/incubator-superset). However, time series Chart is not compatible as snappydata always return the columns' name in upper case, while Superset's key "DTTM_ALIAS" is lower case "__datetime".


### Fix the Incompatible code


A few lines need patch to the file superset/viz.py

``` python
def get_df(self, query_obj=None):
    """Returns a pandas dataframe based on the query object"""
...
    if df is None or df.empty:
        self.status = utils.QueryStatus.FAILED
        if not self.error_message:
            self.error_message = 'No data.'
            return pd.DataFrame() 
        else:
            _tmp = []
            for _c in df.columns:
                if _c.lower() == DTTM_ALIAS:
                    _tmp.append(DTTM_ALIAS)
                else:
                    _tmp.append(_c)
            df.columns = _tmp
```
