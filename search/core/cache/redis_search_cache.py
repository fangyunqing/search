# @Time    : 2023/03/11 14:50
# @Author  : fyq
# @File    : redis_search_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import math
from abc import ABCMeta, abstractmethod
from typing import List, Any

import polars as pl
import redis
import simplejson as json
from loguru import logger
from redis import Redis

from search import constant
from search.core.json_encode import SearchEncoder
from search.core.page import Page
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.exceptions import SearchException
from search.extend import redis_pool


class RedisSearchCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:
        pass

    @abstractmethod
    def set_data(self, search_context: SearchContext, data_df: pl.DataFrame, page_begin: int = 0, whole: bool = True):
        pass


class AbstractRedisSearchCache(RedisSearchCache):

    def __init__(self):
        pass

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

    def set_data(self, search_context: SearchContext, data_df: pl.DataFrame, page_begin: int = 1, whole: bool = True):
        r = redis.Redis(connection_pool=redis_pool)
        self.count(search_context=search_context, data_df=data_df)
        self.exec(r=r,
                  search_context=search_context,
                  data_df=data_df,
                  page_number=page_begin)

        # for index in range(0, self.count(search_context=search_context, data_df=data_df)):
        #     chunk_df: pl.DataFrame = data_df.slice(page_size * index, page_size)
        #     if len(chunk_df) > 0:
        #         self.exec(r=r,
        #                   search_context=search_context,
        #                   chunk_df=chunk_df,
        #                   page_number=page_begin)
        #         page_begin += 1

        value: bytes = r.get(name=f"{search_context.search_key}_{constant.CSV}")
        page_size = search_context.search.page_size
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
    def count(self, search_context: SearchContext, data_df: pl.DataFrame):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, r: Redis, data_df: pl.DataFrame, page_number: int):
        pass


class CommonRedisSearchCache(AbstractRedisSearchCache):
    execs = ["exec"]

    def count(self, search_context: SearchContext, data_df: pl.DataFrame):
        page_size = search_context.search.page_size
        return 1

    def exec(self, search_context: SearchContext, r: Redis, data_df: pl.DataFrame, page_number: int):
        page_size = search_context.search.page_size
        number = math.ceil(len(data_df) / page_size)
        with r.pipeline(transaction=True) as pipe:
            for index in range(0, number):
                chunk_df: pl.DataFrame = data_df.slice(page_size * index, page_size)
                if len(chunk_df) > 0:
                    redis_key: str = f"{search_context.search_key}_{page_number}"
                    data = json.dumps(chunk_df.to_dicts(), cls=SearchEncoder, ignore_nan=True)
                    pipe.setex(name=redis_key,
                               time=search_context.search.redis_cache_time,
                               value=data)
                    page_number += 1
            pipe.execute()


@Progress(prefix="search", suffix="redis_o_file")
class DefaultRedisSearchCache(CommonRedisSearchCache):
    pass
