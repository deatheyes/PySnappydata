from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import  re

from sqlalchemy.engine import default

from pysnappydata import snappydata

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler

from sqlalchemy import types as sqltypes
from sqlalchemy import exc
from SDTCLIService import ttypes

_logger = logging.getLogger(__name__)

ischema_names = {
    'BIGINT': sqltypes.BIGINT,
    'BLOB': sqltypes.BLOB,
    'BOOL': sqltypes.BOOLEAN,
    'BOOLEAN': sqltypes.BOOLEAN,
    'CHAR': sqltypes.CHAR,
    'DATE': sqltypes.DATE,
    'DATE_CHAR': sqltypes.DATE,
    'DATETIME': sqltypes.DATETIME,
    'DATETIME_CHAR': sqltypes.DATETIME,
    'DOUBLE': sqltypes.FLOAT,
    'DECIMAL': sqltypes.DECIMAL,
    'FLOAT': sqltypes.FLOAT,
    'INT': sqltypes.INTEGER,
    'INTEGER': sqltypes.INTEGER,
    'NUMERIC': sqltypes.NUMERIC,
    'REAL': sqltypes.REAL,
    'SMALLINT': sqltypes.SMALLINT,
    'TEXT': sqltypes.TEXT,
    'TIME': sqltypes.TIME,
    'TIME_CHAR': sqltypes.TIME,
    'TIMESTAMP': sqltypes.TIMESTAMP,
    'VARCHAR': sqltypes.VARCHAR,
    'NVARCHAR': sqltypes.NVARCHAR,
    'NCHAR': sqltypes.NCHAR,
    'STRING': sqltypes.TEXT,
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

    def _resolve_type_affinity(self, type_):
        match = re.match(r'([\w ]+)(\(.*?\))?', type_)
        if match:
            coltype = match.group(1)
            args = match.group(2)
        else:
            coltype = ''
            args = ''

        if coltype in ischema_names:
            coltype = ischema_names[coltype]
        elif 'INT' in coltype:
            coltype = sqltypes.INTEGER
        elif 'CHAR' in coltype or 'CLOB' in coltype or 'TEXT' in coltype:
            coltype = sqltypes.TEXT
        elif 'BLOB' in coltype or not coltype:
            coltype = sqltypes.NullType
        elif 'REAL' in coltype or 'FLOA' in coltype or 'DOUB' in coltype:
            coltype = sqltypes.REAL
        else:
            coltype = sqltypes.TEXT

        if args is not None:
            args = re.findall(r'(\d+)', args)
            try:
                coltype = coltype(*[int(a) for a in args])
            except TypeError:
                _logger.warn(
                    "Could not instantiate type %s with "
                    "reflected arguments %s; using no arguments.",
                    coltype, args)
                coltype = coltype()
        else:
            coltype = coltype()
        return coltype

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

    def _get_columns_info(self, col_name, col_type, _comment):
        coltype = self._resolve_type_affinity(col_type)
        return {
            'name': col_name,
            'type': coltype,
            'nullable': True,
            'default': None,
            'autoincrement': 'auto',
        }

    def get_columns(self, connection, table_name, schema=None, **kw):
        # TODO: parse auto increament and sequence, thrift client support needed.
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, col_type, _comment) in rows:
            # Different from the interface description, see 'sqlalchemy/dialects/sqlite/base.py' for more detail.
            result.append(self._get_columns_info(col_name, col_type.upper(), _comment));
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
        #return [row[0] for row in connection.execute('SHOW SCHEMAS')]
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # TODO
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # TODO
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # TODO
        return []
