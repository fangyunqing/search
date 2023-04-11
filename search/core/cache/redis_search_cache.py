# @Time    : 2023/03/11 14:50
# @Author  : fyq
# @File    : redis_search_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import math
from abc import ABCMeta, abstractmethod

import redis
import simplejson as json
from redis import Redis

from search import constant
from typing import List, Any, Dict

from search.core.json_encode import SearchEncoder
from search.core.page import Page
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.exceptions import SearchException
from search.extend import redis_pool
import pandas as pd


class RedisSearchCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:
        pass

    @abstractmethod
    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, page_begin: int = 0, whole: bool = True):
        pass


class AbstractRedisSearchCache(RedisSearchCache):

    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:
        redis_key: str = f"{search_context.search_key}_{page_number}"
        data = None
        page = None
        value: bytes = redis.Redis(connection_pool=redis_pool).get(name=redis_key)
        if value:
            data = value.decode()
            data = json.loads(data)
        redis_key: str = f"{search_context.search_key}_{constant.TOTAL}"
        value: bytes = redis.Redis(connection_pool=redis_pool).get(name=redis_key)
        if value:
            page = value.decode()
            page = json.loads(page)
            try:
                pages = int(page.get("pages"))
                if pages < page_number:
                    raise SearchException(f"{page_number}超过页数{pages}")
            except ValueError:
                pass
            page["number"] = str(page_number)
        return [data, page]

    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, page_begin: int = 1, whole: bool = True):
        r = redis.Redis(connection_pool=redis_pool)
        page_size = search_context.search.page_size
        for index in range(0, self.count(search_context=search_context, data_df=data_df)):
            chunk_df: pd.DataFrame = data_df.iloc[page_size * index:page_size * (index + 1)]
            if len(chunk_df) > 0:
                self.exec(r=r, search_context=search_context,
                          chunk_df=chunk_df, page_number=page_begin)
                page_begin += 1

        r = redis.Redis(connection_pool=redis_pool)
        value: bytes = r.get(name=f"{search_context.search_key}_{constant.CSV}")
        if value:
            page = value.decode()
            page = json.loads(page)
        else:
            page = Page()
            page.size = page_size
            if whole:
                page.total = str(len(data_df))
                page.pages = str(math.ceil(len(data_df) / page_size))
                page = page.to_dict()
            else:
                page.total = "???"
                page.pages = "???"
                page = page.to_dict()

        r.setex(name=f"{search_context.search_key}_{constant.TOTAL}",
                value=json.dumps(page),
                time=search_context.search.redis_cache_time)

    @abstractmethod
    def count(self, search_context: SearchContext, data_df: pd.DataFrame):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, r: Redis, chunk_df: pd.DataFrame, page_number: int):
        pass


class CommonRedisSearchCache(AbstractRedisSearchCache):
    execs = ["exec"]

    def count(self, search_context: SearchContext, data_df: pd.DataFrame):
        page_size = search_context.search.page_size
        return math.ceil(len(data_df) / page_size)

    def exec(self, search_context: SearchContext, r: Redis, chunk_df: pd.DataFrame, page_number: int):
        data = json.dumps(chunk_df.to_dict("records"), cls=SearchEncoder, ignore_nan=True)
        redis_key: str = f"{search_context.search_key}_{page_number}"
        r.setex(name=redis_key,
                time=search_context.search.redis_cache_time,
                value=data)


@Progress(prefix="search", suffix="redis_o_csv")
class DefaultRedisSearchCache(CommonRedisSearchCache):
    pass