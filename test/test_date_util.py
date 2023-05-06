# @Time    : 2023/05/06 10:50
# @Author  : fyq
# @File    : test_date_util.py
# @Software: PyCharm

__author__ = 'fyq'

import unittest
from datetime import datetime
from dateutil import rrule


class TestDateUtil(unittest.TestCase):

    def test_rrule(self):
        b = "2023-04-07"
        e = "2023-05-07"
        bd = datetime.strptime(b, "%Y-%m-%d")
        ed = datetime.strptime(e, "%Y-%m-%d")
        mouths = rrule.rrule(freq=rrule.DAILY,
                             dtstart=ed,
                             until=bd).count()
        print(mouths)
