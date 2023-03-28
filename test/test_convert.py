# @Time    : 2023/03/22 10:37
# @Author  : fyq
# @File    : test_convert.py
# @Software: PyCharm

__author__ = 'fyq'

import unittest

from search.util.convert import data2obj


class TestConvert(unittest.TestCase):

    def test_data2obj(self):

        class body:

            def __init__(self):
                self.age = None
                self.name = None
                self.address = None
                self.phone = None

        data = {
            "age": 19,
            "name": "fyq",
            "address": "福建莆田",
            "phone": "18750767178"
        }

        obj: body = data2obj(data, body)
        self.assertIsInstance(obj, body)