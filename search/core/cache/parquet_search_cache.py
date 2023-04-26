# @Time    : 2023/03/11 15:16
# @Author  : fyq
# @File    : csv_search_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import math
import os
import uuid
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import List, Any

import polars as pl
import redis
import simplejson as json
from sqlalchemy import desc

from search import models, constant, db
from search.core.cache import CommonRedisSearchCache
from search.core.page import Page
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.exceptions import FileNotFindSearchException, SearchException
from search.extend import redis_pool


class ParquetSearchCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:
        pass

    @abstractmethod
    def set_data(self, search_context: SearchContext, data_df: pl.DataFrame, file_dir: str):
        pass


class AbstractParquetSearchCache(ParquetSearchCache):

    def __init__(self):
        self.number_of_pages = 50

    def get_data(self, search_context: SearchContext, page_number: int) -> List[Any]:

        all_pages = search_context.search.pages * self.number_of_pages
        order = math.ceil(page_number / all_pages) - 1
        search_file: models.SearchFile = \
            models.SearchFile.query.filter_by(search_md5=search_context.search_key,
                                              use=constant.SEARCH,
                                              status=constant.FileStatus.USABLE,
                                              order=order) \
                                   .order_by(desc(models.SearchFile.create_time)) \
                                   .first()
        if search_file and os.path.isfile(search_file.path):
            relative_page_number = page_number - order * all_pages
            begin_page = None
            begin_row = None
            for _ in range(0, all_pages, search_context.search.pages):
                if _ + search_context.search.pages > relative_page_number >= _:
                    begin_page = _ + order * all_pages + 1
                    begin_row = _ * search_context.search.page_size
                    break

            if begin_page is None or begin_row is None:
                raise SearchException("超过最大页数")

            df = pl.scan_parquet(search_file.path).\
                slice(begin_row, search_context.search.pages * search_context.search.page_size).collect()

            d_redis_search_cache = CommonRedisSearchCache()
            d_redis_search_cache.set_data(search_context=search_context,
                                          data_df=df,
                                          page_begin=begin_page,
                                          whole=False)
            data, page = d_redis_search_cache.get_data(search_context=search_context,
                                                       page_number=page_number)
            page["number"] = str(page_number)
        else:
            raise FileNotFindSearchException
        return [data, page]

    def set_data(self, search_context: SearchContext, data_df: pl.DataFrame, file_dir: str):
        cache_size = search_context.search.page_size * search_context.search.pages * self.number_of_pages
        spilt_len = math.ceil(len(data_df) / cache_size)
        self.count(spilt_len)
        search_file_list: List[models.SearchFile] = []
        file_path_list: List[str] = []
        file_prefix = uuid.uuid4()
        dt = datetime.now()
        try:
            for data_df_index in range(0, spilt_len):
                file_name = f"{file_prefix}-{data_df_index}.parquet"
                file_path = f"{file_dir}{os.sep}{file_name}"
                chunk_df = data_df.slice(data_df_index * cache_size, cache_size)
                self.exec(search_context=search_context, data_df=chunk_df, file_path=file_path)
                search_file = models.SearchFile()
                search_file.path = file_path
                search_file.search_md5 = search_context.search_key
                search_file.use = constant.SEARCH
                search_file.size = os.path.getsize(file_path)
                search_file.file_name = file_name
                search_file.search_id = search_context.search.id
                search_file.order = data_df_index
                search_file.create_time = dt
                search_file_list.append(search_file)
                file_path_list.append(file_path)
            db.session.add_all(search_file_list)
            db.session.commit()
        except Exception as e:
            [os.remove(fp, dir_fd=None) for fp in file_path_list]
            raise e
        self.exec_page(search_context=search_context, data_df=data_df)

    @abstractmethod
    def count(self, split_len: int):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, data_df: pl.DataFrame, file_path: str):
        pass

    @abstractmethod
    def exec_page(self, search_context: SearchContext, data_df: pl.DataFrame):
        pass


@Progress(prefix="search", suffix="redis_o_csv")
class DefaultParquetSearchCache(AbstractParquetSearchCache):
    execs = ["exec", "exec_page"]

    def count(self, split_len: int):
        return split_len + 1

    def exec(self, search_context: SearchContext, data_df: pl.DataFrame, file_path: str):
        data_df.write_parquet(file_path)

    def exec_page(self, search_context: SearchContext, data_df: pl.DataFrame):
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
