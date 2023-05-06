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

        df = pl.LazyFrame(data=data, schema={"sColorName": pl.Utf8, "naa": pl.Decimal, "uuid": pl.Object})

        print(df.collect().to_pandas())

    def test_read_json(self):

        import ndjson
        posts = []
        for _ in range(0, 2000000):
            posts.append({"a": 3, "b": 4, "c": "3", "d": 3, "e": 4, "f": "3", "g": 3, "h": 4, "i": "3"})
        with open('data.ndjson', 'w') as f:
            writer = ndjson.writer(f, ensure_ascii=False)

            for post in posts:
                writer.writerow(post)
        df = pl.scan_ndjson("data.ndjson")
        print(df.collect())
        pl.DataFrame().write_ndjson()

    def test_to_list(self):
        posts = []
        for _ in range(0, 5000):
            posts.append({"a": 3, "b": 4, "c": "3"})
        df = pl.DataFrame(data=posts)
