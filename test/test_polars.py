# @Time    : 2023/04/21 13:12
# @Author  : fyq
# @File    : test_polars.py
# @Software: PyCharm

__author__ = 'fyq'

import unittest
from datetime import datetime, timedelta

import polars as pl
import numpy as np


class TestPolars(unittest.TestCase):

    def test_join(self):
        df1 = pl.DataFrame(
            {
                "y": ["A", "A", "A", "B", "B", "B", "C", "X"],
                "b": np.random.rand(8),
                "c": [datetime(2022, 12, 1) + timedelta(days=idx) for idx in range(8)],
                "d": [1, 2.0, np.NaN, np.NaN, 0, -5, -42, None],
            }
        )

        df2 = pl.DataFrame({"x": np.arange(0, 8), "y": ["A", "A", "A", "B", "B", "C", "X", "X"]})

        print(df1.join(df2, on="y", ))
