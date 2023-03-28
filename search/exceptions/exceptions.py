# @Time    : 23/02/22 13:32
# @Author  : fyq
# @File    : exceptions.py
# @Software: PyCharm

__author__ = 'fyq'


class SearchException(Exception):
    pass


class ProgressException(SearchException):
    pass


class SearchUtilException(SearchException):
    pass
