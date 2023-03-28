# @Time    : 23/02/22 9:15
# @Author  : fyq
# @File    : db_type.py
# @Software: PyCharm

__author__ = 'fyq'


class DataBaseType:
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"
    MYSQL = "mysql"

    def __new__(cls, *args, **kwargs):
        raise NotImplemented
