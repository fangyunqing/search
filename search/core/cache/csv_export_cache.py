# @Time    : 2023/03/11 16:12
# @Author  : fyq
# @File    : csv_export_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import math
import os
import uuid
from abc import ABCMeta, abstractmethod
import pandas as pd
from typing import Optional

import redis
from sqlalchemy import desc

from search import models, constant, db
from search.core.page import Page
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.extend import redis_pool

import simplejson as json


class CSVExportCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        pass

    @abstractmethod
    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        pass


class AbstractCSVExportCache(CSVExportCache):

    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        search_file: models.SearchFile = \
            models.SearchFile.query.filter_by(search_md5=search_context.search.search_md5, use=constant.SEARCH) \
            .order_by(desc(models.SearchFile.create_time)) \
            .first()
        if search_file and os.path.isfile(search_file.path):
            return pd.read_csv(filepath_or_buffer=search_file.path,
                               sep="`")

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


@Progress(prefix="export", suffix="csv")
class DefaultCSVExportCache(AbstractCSVExportCache):
    execs = ["exec", "exec_page"]

    def count(self):
        return 2

    def exec(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        search_file = models.SearchFile()
        file_path = f"{file_dir}{os.sep}{uuid.uuid4()}.csv"
        data_df.to_csv(file_path, sep="`", index=False)
        d, f = os.path.split(file_path)
        search_file.path = file_path
        search_file.search_md5 = search_context.search_md5.search_md5
        search_file.use = constant.FileUse.SEARCH
        search_file.size = os.path.getsize(file_path)
        search_file.file_name = f
        db.session.add(search_file)
        db.session.commit()

    def exec_page(self, search_context: SearchContext, data_df: pd.DataFrame):
        page = Page()
        page.size = str(search_context.search.page_size)
        page.total = str(len(data_df))
        page.pages = str(math.ceil(len(data_df) / search_context.search.page_size))

        r = redis.Redis(connection_pool=redis_pool)
        r.setex(name=f"{search_context.search_md5.search_md5}_{constant.CSV}",
                value=json.dumps(page.to_dict()),
                time=search_context.search.redis_cache_time)
