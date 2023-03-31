# @Time    : 2023/03/11 15:16
# @Author  : fyq
# @File    : csv_search_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import math
import os
import uuid
from abc import ABCMeta, abstractmethod
from typing import List, Any
import pandas as pd
import redis
from sqlalchemy import desc

from search import models, constant, db
from search.core.cache import DefaultRedisSearchCache
from search.core.page import Page
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.extend import redis_pool

import simplejson as json


class CSVSearchCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:
        pass

    @abstractmethod
    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        pass


class AbstractCSVSearchCache(CSVSearchCache):

    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:
        search_file: models.SearchFile = \
            models.SearchFile.query.filter_by(search_md5=search_context.search_key, use=constant.SEARCH) \
            .order_by(desc(models.SearchFile.create_time)) \
            .first()
        data = None
        page = None
        if search_file and os.path.isfile(search_file.path):
            pages = search_context.search.pages
            page_size = search_context.search.page_size
            page_begin = page_number - pages if page_number - pages > 0 else 1
            page_end = page_number + pages
            df = pd.read_csv(search_file.path,
                             sep="`",
                             skiprows=range(1, (page_begin - 1) * page_size + 1),
                             nrows=page_size * (page_end - page_begin + 1) + 1)
            d_redis_search_cache = DefaultRedisSearchCache()
            d_redis_search_cache.set_data(search_context=search_context,
                                          data_df=df,
                                          page_begin=page_begin,
                                          whole=False)
            data, page = d_redis_search_cache.get_data(search_context=search_context,
                                                       page_number=page_number)
            page["number"] = str(page_number)

        return [data, page]

    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        self.count()
        self.exec(search_context=search_context, data_df=data_df, file_dir=file_dir)
        self.exec_page(search_context=search_context, data_df=data_df)

    @abstractmethod
    def count(self):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        pass

    @abstractmethod
    def exec_page(self, search_context: SearchContext, data_df: pd.DataFrame):
        pass


@Progress(prefix="search", suffix="csv")
class DefaultCSVSearchCache(AbstractCSVSearchCache):
    execs = ["exec", "exec_page"]

    def count(self):
        return 2

    def exec(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        search_file = models.SearchFile()
        file_path = f"{file_dir}{os.sep}{uuid.uuid4()}.csv"
        data_df.to_csv(file_path, sep="`", index=False)
        d, f = os.path.split(file_path)
        search_file.path = file_path
        search_file.search_md5 = search_context.search_key
        search_file.use = constant.SEARCH
        search_file.size = os.path.getsize(file_path)
        search_file.file_name = f
        search_file.search_id = search_context.search.id
        db.session.add(search_file)
        db.session.commit()

    def exec_page(self, search_context: SearchContext, data_df: pd.DataFrame):
        page = Page()
        page.size = str(search_context.search.page_size)
        page.total = str(len(data_df))
        page.pages = str(math.ceil(len(data_df) / search_context.search.page_size))

        r = redis.Redis(connection_pool=redis_pool)
        r.set(name=f"{search_context.search_key}_{constant.CSV}",
              value=json.dumps(page.to_dict()))

        r.setex(name=f"{search_context.search_key}_{constant.TOTAL}",
                value=json.dumps(page.to_dict()),
                time=search_context.search.redis_cache_time)
