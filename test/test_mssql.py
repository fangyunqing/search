# @Time    : 2023/05/03 9:34
# @Author  : fyq
# @File    : test_mssql.py
# @Software: PyCharm

__author__ = 'fyq'

import os

import simplejson
from pymssql import _mssql
import pymssql
import unittest
import psutil
import polars as pl

from search.core.json_encode import SearchEncoder


class TestMssql(unittest.TestCase):

    def test_mssql(self):
        conn = pymssql.connect(host="10.103.88.88",
                               user="pdauser",
                               password="pda2018@#",
                               charset="utf8",
                               database="HSWarpERP",
                               port=1433)
        cur = conn.cursor(as_dict=True)
        try:
            p = psutil.Process()
            print(p.memory_full_info().uss / 1024 / 1024)
            sql = "SELECT  a.tCreateTime," \
                  "       a.sContractNo," \
                  "       a.sBarnd," \
                  "       a.sSalesGroupName," \
                  "       a.sSalesName," \
                  "       a.iBillStatus," \
                  "       a.tAuditTime," \
                  "       a.nTaxRate," \
                  "       a.sContractType" \
                  " FROM dbo.sdSellContractHdr a WITH (NOLOCK)"
            " WHERE a.tCreateTime >= '2021-01-01'" \
            "  AND a.tCreateTime <= '2023-01-01'" \
            "  AND a.bUsable = 1" \
            " ORDER BY a.tCreateTime DESC"
            cur.execute(sql)

            index = 0
            while True:
                datas = cur.fetchmany(5000)
                if not datas:
                    break
                pl.DataFrame(datas).write_parquet(f"{index}.parquet")
                index += 1
                print(p.memory_full_info().uss / 1024 / 1024)


        finally:
            conn.close()
