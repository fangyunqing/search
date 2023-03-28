# @Time    : 2023/03/23 10:24
# @Author  : fyq
# @File    : transactional.py
# @Software: PyCharm

__author__ = 'fyq'

import functools

from search import db


def transactional(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            res = f(*args, **kwargs)
            db.session.commit()
            return res
        except Exception as e:
            db.session.rollback()
            raise e

    return wrapper
