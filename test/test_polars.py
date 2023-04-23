# @Time    : 2023/04/22 15:31
# @Author  : fyq
# @File    : test_polars.py
# @Software: PyCharm

__author__ = 'fyq'

import unittest
import uuid
from decimal import Decimal

import polars as pl


class TestPolars(unittest.TestCase):

    def test_df(self):

        pl.Config(activate_decimals=True, set_fmt_str_lengths=100)

        data = []
        for _ in range(0, 100):
            data.append((None, Decimal("13.99008"), uuid.uuid4()))

        data.append(("ABC 黑色", None, uuid.uuid4()))

        for _ in range(0, 100):
            data.append((None, None, uuid.uuid4()))

        df = pl.LazyFrame(data=data, schema={"sColorName": pl.Utf8, "naa": pl.Decimal,  "uuid": pl.Object})

        def abc(row):
            pass

        print(df.collect().to_pandas())
