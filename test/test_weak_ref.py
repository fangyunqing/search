# @Time    : 2023/03/31 15:30
# @Author  : fyq
# @File    : test_weakR_ref.py
# @Software: PyCharm

__author__ = 'fyq'


import unittest
import weakref


class TestWeakRef(unittest.TestCase):

    def test_ref(self):

        class Body:

            def __init__(self):
                self.c = 3

        a = {
            "a": Body()
        }

        b = weakref.ref(a["a"])

        print(b())

        a.pop("a")

        print(b())

