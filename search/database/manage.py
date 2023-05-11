# @Time    : 23/02/22 8:47
# @Author  : fyq
# @File    : manage.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import Dict, List

import pymssql
from dbutils.pooled_db import PooledDB
from sortedcontainers import SortedKeyList

from search.entity import DataBaseType
from search.exceptions import SearchException
from search.models import SearchDatasource
import munch
import pyodbc


class DataBasePool:
    SUPPORTS = [DataBaseType.ORACLE, DataBaseType.SQLSERVER, DataBaseType.MYSQL]

    def __init__(self,
                 db_type: str,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 db_name,
                 **kwargs):

        if db_type not in self.SUPPORTS:
            raise SearchException(f"不支持{db_type}数据库")

        config = {}
        creator = None
        if DataBaseType.MYSQL == db_type:
            pass
        elif DataBaseType.SQLSERVER == db_type:
            creator = pyodbc
            config["dsn"] = "MSSQL"
            config["server"] = host
            config["port"] = port
            config["user"] = user
            config["password"] = password
            config["database"] = db_name
            config["atuocommit"] = True
            config["ColumnEncryption"] = "Enabled"
            # # query timeout in seconds, default 0 (no timeout)
            # config["timeout"] = kwargs.pop("timeout", 0)
            # # timeout for connection and login in seconds, default 60
            # config["login_timeout"] = kwargs.pop("login_timeout", 60)
            # # # whether rows should be returned as dictionaries instead of tuples.
            # # config["as_dict"] = kwargs.pop("as_dict", True)
            # # Set the application name to use for the connection
            # config["appname"] = kwargs.pop("appname", None)
            # # Whether to use default autocommit mode or not
            # config["autocommit"] = kwargs.pop("autocommit", None)
            # # TDS protocol version to use.
            # config["tds_version"] = kwargs.pop("tds_version", None)
            # # 编码
            # config["charset"] = kwargs.pop("charset", "utf8")
            # # 结果类型
            # config["as_dict"] = True
        elif DataBaseType.ORACLE == db_type:
            pass

        # initial number of idle connections in the pool
        # (0 means no connections are made at startup)
        mincached = kwargs.pop("mincached", 0)
        # maximum number of idle connections in the pool
        # (0 or None means unlimited pool size)
        maxcached = kwargs.pop("mincached", 80)
        # maximum number of shared connections
        # When this maximum number is reached, connections are
        # shared if they have been requested as shareable.
        maxshared = kwargs.pop("maxshared", 6)
        # maximum number of connections generally allowed
        # (0 or None means an arbitrary number of connections)
        maxconnections = kwargs.pop("maxconnections", 80)
        # determines behavior when exceeding the maximum
        # (if this is set to true, block and wait until the number of
        # connections decreases, otherwise an error will be reported)
        blocking = kwargs.pop("blocking", True)
        # maximum number of reuses of a single connection
        # (0 or None means unlimited reuse)
        # When this maximum usage number of the connection is reached,
        # the connection is automatically reset (closed and reopened).
        maxusage = kwargs.pop("maximum", 10)
        # optional list of SQL commands that may serve to prepare
        # the session, e.g. ["set datestyle to ...", "set time zone ..."]
        setsession = kwargs.pop("setsession", None)
        # how connections should be reset when returned to the pool
        # (False or None to rollback transcations started with begin(),
        # True to always issue a rollback for safety's sake)
        reset = kwargs.pop("reset", True)
        # determines when the connection should be checked with ping()
        # (0 = None = never, 1 = default = whenever fetched from the pool,
        # 2 = when a cursor is created, 4 = when a query is executed,
        # 7 = always, and all other bit combinations of these values)"""
        ping = kwargs.pop("ping", 0)

        self._pool = PooledDB(creator=creator,
                              mincached=mincached,
                              maxcached=maxcached,
                              maxshared=maxshared,
                              maxconnections=maxconnections,
                              blocking=blocking,
                              maxusage=maxusage,
                              setsession=setsession,
                              reset=reset,
                              ping=ping,
                              **config)

    @property
    def connection(self):
        return self._pool.connection()


class DataBaseManage:

    def __init__(self):
        self._pools: Dict[str, DataBasePool] = {}
        self._datasource_list = SortedKeyList(key=lambda ds: ds.order)

    def register(self, ds: SearchDatasource):
        if ds.name in self._pools:
            tmp = self._pools.pop(ds.name)
            if tmp:
                del tmp
            for tmp_index, tmp in enumerate(self._datasource_list):
                if ds.name == tmp.name:
                    self._datasource_list.pop(tmp_index)
                    break

        self._pools[ds.name] = DataBasePool(db_type=ds.db_type,
                                            user=ds.user_name,
                                            password=ds.password,
                                            host=ds.ip,
                                            port=ds.port,
                                            db_name=ds.db_name,
                                            as_dict=True)
        self._datasource_list.add(munch.Munch(ds.to_dict()))

    def unregister(self, name: str):
        if name in self._pools:
            tmp = self._pools.pop(name)
            if tmp:
                del tmp
            for tmp_index, tmp in enumerate(self._datasource_list):
                if name == tmp.name:
                    self._datasource_list.pop(tmp_index)
                    break

    def get_connections(self) -> List:
        return [self._pools[ds.name].connection for ds in self._datasource_list]

    def get_main_connection(self):
        if len(self._datasource_list) > 0:
            return self._pools[self._datasource_list[0].name].connection


dm: DataBaseManage = DataBaseManage()
