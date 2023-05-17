# @Time    : 2023/04/25 14:31
# @Author  : fyq
# @File    : search_strategy.py
# @Software: PyCharm

__author__ = 'fyq'

import functools
from abc import ABCMeta, abstractmethod
from typing import List

import redis
import simplejson
from loguru import logger
from munch import Munch

from search import constant
from search.core.search_context import SearchContext
from search.exceptions import SearchStrategyException
from search.extend import redis_pool
import sys
from search.util.redis import redis_lock, redis_search_config


class ISearchStrategy(metaclass=ABCMeta):

    @abstractmethod
    def add_lock(self, f):
        pass

    @abstractmethod
    def can_search(self, search_context: SearchContext) -> bool:
        pass


class DefaultStrategySearch(ISearchStrategy):

    def __init__(self):
        self._default = Munch({
            "l": 0,
            "m": 0,
            "s": 0,
            "ss": 0
        })

    def add_lock(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            search_context: SearchContext = kwargs.get("search_context")
            if search_context:
                top = kwargs.get("top")
                with redis_lock(constant.RedisKey.SEARCH_STRATEGY_LOCK, forever=True):
                    r = redis.Redis(connection_pool=redis_pool)
                    # 查询策略
                    value: bytes = r.get(constant.RedisKey.SEARCH_STRATEGY)
                    if value:
                        data = Munch(simplejson.loads(value.decode()))
                    else:
                        data = self._default

                    # 查询配置
                    config = redis_search_config("l", "s", "m", "ss")
                    if config is None:
                        config = Munch()

                    if top:
                        data.ss += 1
                        ss = config.setdefault("ss", sys.maxsize)
                        if data.ss > ss:
                            logger.error(f"查询策略[ss]已经超过{ss}")
                            raise SearchStrategyException()
                    else:
                        weight = search_context.score[1]
                        max_count = config.setdefault(weight, sys.maxsize)
                        data[weight] += 1
                        if data[weight] > max_count:
                            logger.error(f"查询策略[{weight}]已经超过{max_count}")
                            raise SearchStrategyException()

                    r.set(constant.RedisKey.SEARCH_STRATEGY, simplejson.dumps(data))

                res = f(*args, **kwargs)

                with redis_lock(constant.RedisKey.SEARCH_STRATEGY_LOCK, forever=True):
                    r = redis.Redis(connection_pool=redis_pool)
                    value: bytes = r.get(constant.RedisKey.SEARCH_STRATEGY)
                    if value:
                        data = Munch(simplejson.loads(value.decode()))
                    else:
                        data = self._default

                    if top:
                        if data.ss > 0:
                            data.ss -= 1
                        else:
                            weight = search_context.score[1]
                            if data[weight] > 0:
                                data[weight] -= 1

                    r.set(constant.RedisKey.SEARCH_STRATEGY, simplejson.dumps(data))

                return res
            else:
                return f(*args, **kwargs)

        return wrapper

    def can_search(self, search_context: SearchContext) -> bool:
        with redis_lock(constant.RedisKey.SEARCH_STRATEGY_LOCK, forever=True):
            r = redis.Redis(connection_pool=redis_pool)
            # 查询策略
            value: bytes = r.get(constant.RedisKey.SEARCH_STRATEGY)
            if value:
                data = Munch(simplejson.loads(value.decode()))
            else:
                data = self._default

            # 查询配置
            config = redis_search_config("l", "s", "m", "ss")
            if config is None:
                config = Munch()

            weight = search_context.score[1]
            max_count = config.setdefault(weight, sys.maxsize)
            return data[weight] + 1 < max_count


search_strategy = DefaultStrategySearch()
