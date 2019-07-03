# DB-API implementation

from __future__ import absolute_import
from __future__ import unicode_literals

# Make all exceptions visible in this module per DB-API
import logging
import sys
import socket
import time

if sys.version_info.major == 2:
    #Python 2
    import thread
else:
    #Python 3
    import _thread as thread

from SDTCLIService import SnappyDataService
from SDTCLIService import ttypes
from SDTCLIService import LocatorService
from pysnappydata import common

import thrift.protocol.TCompactProtocol
import thrift.transport.TSocket
from pysnappydata.exc import *

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

    def __init__(self, host, port=1528, username=None, password=None, locator=False):
        if locator:
            _logger.info("connect to locator %s:%d", host, port)
            tsocket = thrift.transport.TSocket.TSocket(host, port)
            iprot = thrift.protocol.TCompactProtocol.TCompactProtocol(tsocket)
            oprot = thrift.protocol.TCompactProtocol.TCompactProtocol(tsocket)
            self._locator = LocatorService.Client(iprot, oprot)
            tsocket.open()
            prefer_server = \
                self._locator.getPreferredServer(
                    serverTypes=set([LocatorService.ServerType.THRIFT_SNAPPY_CP]),
                    serverGroups=None,
                    failedServers=None)
            self._hostname = prefer_server.hostName
            self._port = prefer_server.port
        else:
            self._hostname = host
            self._port = port

        _logger.info("connect to server %s:%d", self._hostname, self._port)
        self._clientid = self._hostname + str(thread.get_ident()) + str(time.time())
        tsocket = thrift.transport.TSocket.TSocket(self._hostname, self._port)
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
        except ttypes.SnappyException as e:
            if isinstance(e.exceptionData, ttypes.SnappyExceptionData) and e.exceptionData.errorCode == 2000:
                pass

    def execute(self, sql, attr=None, outputparams=None):
        return self._client.execute(self._conn_properties.connId, sql, outputparams, attr, self._conn_properties.token)

    def get_next_result_set(self, cursorid):
        return self._client.getNextResultSet(cursorid, None, self._conn_properties.token)


class Cursor(common.DBAPICursor):
    def __init__(self, connection, arraysize=1000):
        self._operationHandle = None
        self._description = None
        super(Cursor, self).__init__()
        self.arraysize = arraysize
        self._connection = connection
        self._rowcount = 0;

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
            self._data += self._build_data()
        self._update_rowcount()

    def _build_data(self):
        data = []
        for row in self._operationHandle.resultSet.rows:
            item = []
            for column, descriptor in zip(row.values, self._operationHandle.resultSet.metadata):
                item.append(self._build_item(column, descriptor))
            data.append(item)
        return data

    def _build_item(self, column, descriptor):
        if column.null_val is not None and column.null_val:
            return None
        if column is None or descriptor is None:
            return None
        if descriptor.type == ttypes.SnappyType.BOOLEAN:
            return column.bool_val
        elif descriptor.type == ttypes.SnappyType.TINYINT:
            return column.byte_val
        elif descriptor.type == ttypes.SnappyType.SMALLINT:
            return column.i16_val
        elif descriptor.type == ttypes.SnappyType.INTEGER:
            return column.i32_val
        elif descriptor.type == ttypes.SnappyType.BIGINT:
            return column.i64_val
        elif descriptor.type == ttypes.SnappyType.FLOAT or descriptor.type == ttypes.SnappyType.DOUBLE:
            return column.double_val
        elif descriptor.type == ttypes.SnappyType.CHAR or descriptor.type == ttypes.SnappyType.VARCHAR or descriptor.type == ttypes.SnappyType.LONGVARCHAR:
            return column.string_val
        elif descriptor.type == ttypes.SnappyType.DECIMAL:
            return column.decimal_val
        elif descriptor.type == ttypes.SnappyType.DATE:
            return column.date_val
        elif descriptor.type == ttypes.SnappyType.TIME:
            return column.time_val
        elif descriptor.type == ttypes.SnappyType.TIMESTAMP:
            return column.timestamp_val
        elif descriptor.type == ttypes.SnappyType.BINARY or descriptor.type == ttypes.SnappyType.VARBINARY or descriptor.type == ttypes.SnappyType.LONGVARBINARY:
            return column.binary_val
        elif descriptor.type == ttypes.SnappyType.BLOB:
            return column.blob_val.chunk
        elif descriptor.type == ttypes.SnappyType.CLOB or descriptor.type == ttypes.SnappyType.JSON or descriptor.type == ttypes.SnappyType.SQLXML:
            return column.clob_val.chunk
        elif descriptor.type == ttypes.SnappyType.ARRAY:
            return self._build_array(column.array_val, descriptor.elementTypes[0])
        elif descriptor.type == ttypes.SnappyType.MAP:
            return self._build_map(column.map_val, descriptor.elementTypes[0], descriptor.elementTypes[1])
        elif descriptor.type == ttypes.SnappyType.STRUCT:
            return self._build_struct(column.struct_val, descriptor.elementTypes)
        elif descriptor.type == ttypes.SnappyType.NULLTYPE:
            return column.null_val
        elif descriptor.type == ttypes.SnappyType.JAVA_OBJECT:
            return column.java_val
        else:
            return column


    def _build_array(self, elems, descriptor):
        ret = []
        for elem in elems:
            ret.append(self._build_item(elem, descriptor))
        return ret

    def _build_map(self, map, descriptor1, descriptor2):
        ret = {}
        for first, second in map.items():
            k = self._build_item(first, descriptor1)
            v = self._build_item(second, descriptor2)
            ret[k] = v
        return ret

    def _build_struct(self, fields, descriptors):
        ret = []
        for field, descriptor in zip(fields, descriptors):
            ret.append(self._build_item(field, descriptor))
        return ret

    def cancel(self):
        self._connection.cancel_current_statement()

    def _update_rowcount(self):
        if self._operationHandle is None:
            return
        if self._operationHandle.updateCount != 0:
            self._rowcount = self._operationHandle.updateCount
        elif self._operationHandle.resultSet is not None:
            self._rowcount = len(self._operationHandle.resultSet.rows)
        else:
            return 0

    @property
    def rowcount(self):
        return self._rowcount

    def nextset(self):
        if self._operationHandle.resultSet.RowSet.cursorId > 0:
            self._operationHandle = self._connection.get_next_result_set()
            del self._data[:]
            self._data += self._operationHandle.resultSet.rows
            self._rownumber = 0

    @property
    def handle(self):
        return self._operationHandle
