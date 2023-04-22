# @Time    : 2023/04/22 15:31
# @Author  : fyq
# @File    : test_polars.py
# @Software: PyCharm

__author__ = 'fyq'

import unittest
import uuid
import polars as pl


class TestPolars(unittest.TestCase):

    def test_df(self):
        data = []
        for _ in range(0, 100):
            data.append((None, uuid.uuid4()))

        data.append(("ABC 黑色", uuid.uuid4()))

        for _ in range(0, 100):
            data.append((None, uuid.uuid4()))

        df = pl.LazyFrame(data=data, schema={"sColorName": pl.Utf8, "uuid": pl.Object}).collect()
        print(df)

