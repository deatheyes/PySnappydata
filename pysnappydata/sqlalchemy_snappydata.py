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

import logging

_logger = logging.getLogger(__name__)

_type_map = {
    'boolean': types.Boolean,
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
            return connection.execute('DESCRIBE {}'.format(full_table))
        except ttypes.SnappyException as e:
            if isinstance(e.exceptionData, ttypes.SnappyExceptionData) and e.exceptionData.errorCode == 20000:
                raise exc.NoSuchTableError(full_table)
            else:
                raise e

    def get_columns(self, connection, table_name, schema=None, **kw):
        # TODO: parse auto increament and sequence
        rows = self._get_table_columns(connection, table_name, schema)
        result = []
        for (col_name, col_type, _comment) in rows:
            try:
                coltype = _type_map[col_type]
            except KeyError:
                _logger.warning("Did not recognize type '%s' of column '%s'", col_type, col_name)
                coltype = types.NullType
            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
                'sequence': {}
            })
        _logger.info("get columns info: %s", result)
        return result

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_view_names(self, connection, schema=None, **kw):
        return self.get_table_names(connection, schema, **kw)

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return [row[0] for row in connection.execute(query)]

    def get_schema_names(self, connection, **kw):
        """ snappydata would throw an exception, prossible a bug
        """
        return [row[0] for row in connection.execute('SHOW SCHEMAS')]

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # TODO
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # TODO
        return []