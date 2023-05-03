# @Time    : 2023/05/03 15:01
# @Author  : fyq
# @File    : search_cost_time.py
# @Software: PyCharm

__author__ = 'fyq'

import functools
import time

from loguru import logger


def search_cost_time(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        begin_time = time.perf_counter()
        res = f(*args, **kwargs)
        end_time = round(time.perf_counter() - begin_time, 3)
        f_info = str(f)
        search_context = kwargs.get("search_context")
        if search_context:
            logger.info(f"{search_context.search_key} {f_info} {end_time}s")
        else:
            logger.info(f"{f_info} {end_time}")
        return res

    return wrapper
