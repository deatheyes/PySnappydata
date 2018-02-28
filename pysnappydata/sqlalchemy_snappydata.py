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
from sqlalchemy import exc
from TCLIService import ttypes

class SnappyDataDialect(default.DefaultDialect):
    name = b'snappydata'
    driver = b'thrift'

    @classmethod
    def dbapi(cls):
        return snappydata

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 1528,
            'username': url.username,
        }
        return [], kwargs

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name

        try:
            cursor = connection.execute('DESCRIBE {}'.format(full_table)).cursor
            return cursor.fetchall(), cursor.description
        except ttypes.SnappyException as e:
            if isinstance(e.exceptionData, ttypes.SnappyExceptionData) and e.exceptionData.errorCode == 20000:
                raise exc.NoSuchTableError(full_table)

    def get_columns(self, connection, table_name, schema=None):
        rows, descriptor = self._get_table_columns(connection, table_name, schema)
        result = []
        for row in rows:
            item = []
            for col, desc in zip (row, descriptor):
                if desc[0] == 'col_name':
                    item['name'] = col.chunk
                elif desc[0] == 'data_type':
                    item['data_type'] = col.chunk
            result.append(item)
        return result

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False



