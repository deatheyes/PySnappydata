from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy.engine import default

from pysnappydata import snappydata

try:
    from sqlalchemy import processors
except ImportError:
    from pysnappydata import sqlalchemy_backports as processors
try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler

from sqlalchemy import types

_type_map = {
    'boolean': types.Boolean,
    'tinyint': types.Integer,
    'smallint': types.SmallInteger,
    'int': types.Integer,
    'bigint': types.BigInteger,
    'float': types.Float,
    'double': types.Float,
    'string': types.String,
    'date': types.Date,
    'timestamp': types.TIMESTAMP,
    'binary': types.String,
    'array': types.String,
    'map': types.String,
    'struct': types.String,
    'uniontype': types.String,
    'decimal': types.DECIMAL,
}


class SnappyDataDialect(default.DefaultDialect):
    name = b'snappydata'
    driver = b'thrift'

    @classmethod
    def dbapi(cls):
        return snappydata

