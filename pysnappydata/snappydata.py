# DB-API implementation

from __future__ import absolute_import
from __future__ import unicode_literals

# Make all exceptions visible in this module per DB-API
import logging
import sys
import socket
import time
import thread

from TCLIService import SnappyDataService
from TCLIService import ttypes
from pysnappydata import common

import thrift.protocol.TCompactProtocol
import thrift.transport.TSocket

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s
_logger = logging.getLogger(__name__)


class SnappyDataParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(
            item
            .replace('\\', '\\\\')
            .replace("'", "\\'")
            .replace('\r', '\\r')
            .replace('\n', '\\n')
            .replace('\t', '\\t')
        )


_escaper = SnappyDataParamEscaper()


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(object):
    """Wraps a Thrift session"""

    def __init__(self, host, port=1528, username=None, password=None):
        self._hostname = socket.gethostname()
        self._clientid = self._hostname + str(thread.get_ident()) + str(time.time())
        tsocket = thrift.transport.TSocket.TSocket(host, port)
        iprot = thrift.protocol.TCompactProtocol.TCompactProtocol(tsocket)
        oprot = thrift.protocol.TCompactProtocol.TCompactProtocol(tsocket)
        tsocket.open()
        arguments = ttypes.OpenConnectionArgs(
            clientHostName=self._hostname,
            clientID=self._clientid,
            userName=username,
            password=password,
            security=ttypes.SecurityMechanism.PLAIN
        )
        self._client = SnappyDataService.Client(iprot, oprot)
        self._conn_properties = self._client.openConnection(arguments)

    def close(self):
        self._client.closeConnection(self._conn_properties.connId, True, self._conn_properties.token)

    def commit(self):
        """By default, autocommit is on"""
        pass

    def rollback(self):
        """By default, autocommit is on"""
        pass

    def cursor(self, *args, **kwargs):
        return Cursor(self, *args, **kwargs)

    @property
    def connectionid(self):
        return self._conn_properties.connId or -1

    @property
    def token(self):
        return self._conn_properties.token or ""

    @property
    def hostname(self):
        return self._hostname

    @property
    def clientid(self):
        return self._clientid

    @property
    def client(self):
        return self._client

    def reset_state(self):
        self._client.closeResultSet(self._conn_properties.connId, self._conn_properties.token)

    def cancel_current_statement(self):
        self._client.cancelCurrentStatement(self._conn_properties.connId, self._conn_properties.token)

    def reset(self):
        try:
            self.reset_state()
            self.cancel_current_statement()
        except ttypes.SnappyException, e:
            if isinstance(e.exceptionData, ttypes.SnappyExceptionData) and e.exceptionData.errorCode == 2000:
                pass
        else:
            raise

    def execute(self, sql, attr=None, outputparams=None):
        return self._client.execute(self._conn_properties.connId, sql, outputparams, attr, self._conn_properties.token)
        #return self._client.prepareAndExecute(
        #    self._conn_properties.connId, sql, params, outputparams, attr, self._conn_properties.token)

    def get_next_result_set(self, cursorid):
        return self._client.getNextResultSet(cursorid, None, self._conn_properties.token)


class Cursor(common.DBAPICursor):
    def __init__(self, connection, arraysize=1000):
        self._operationHandle = None
        self._description = None
        super(Cursor, self).__init__()
        self.arraysize = arraysize
        self._connection = connection

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._description = None
        if self._operationHandle is not None:
            try:
                self._connection.reset()
            finally:
                self._operationHandle = None

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._operationHandle is None or self._operationHandle.resultSet is None:
            return None
        if self._description is None:
            meta = self._operationHandle.resultSet.metadata
            self._description = []
            for col in meta:
                name = col.name.decode('utf-8') if sys.version_info[0] == 2 else col.name
                type = ttypes.SnappyType._VALUES_TO_NAMES[col.type]
                if sys.version_info == 2:
                    type = type.decode('utf-8')
                self._description.append((name, type, None, None, col.precision, col.nullable))
        return self._description

    def close(self):
        """Close the operation handle"""
        self._reset_state()

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        _logger.info('%s', sql)
        self._operationHandle = self._connection.execute(sql)
        if self._operationHandle is not None and self._operationHandle.resultSet is not None:
            self._data += self._operationHandle.resultSet.rows

    def cancel(self):
        self._connection.cancel_current_statement()

    def rowcount(self):
        if self._operationHandle is None or self._operationHandle.resultSet is None:
            return 0
        return self._operationHandle.resultSet.batchUpdateCounts

    def nextset(self):
        if self._operationHandle.resultSet.RowSet.cursorId > 0:
            self._operationHandle = self._connection.get_next_result_set()
            del self._data[:]
            self._data += self._operationHandle.resultSet.rows
            self._rownumber = 0

    @property
    def handle(self):
        return self._operationHandle