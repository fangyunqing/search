# @Time    : 2023/03/28 9:18
# @Author  : fyq
# @File    : test_sql_parser.py
# @Software: PyCharm

__author__ = 'fyq'

import unittest
from search.core.parser.sql_parser import SearchSqlParser


class TestSqlParser(unittest.TestCase):

    def test_sql_parser(self):
        sql = "SELECT *" \
              " FROM dbo.vwmmSTInStore WITH(NOLOCK) " \
              "where ( tStoreInTime >= result.tStoreInBeginTime AND tStoreInTime <= condition.tStoreInEndTime) " \
              "AND sStoreInType like '%车间生产入库%' limit 100"
        s = SearchSqlParser()
        print(s.parse(sql))
