# @Time    : 2023/03/11 16:12
# @Author  : fyq
# @File    : csv_export_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import os
from abc import ABCMeta, abstractmethod
from typing import Optional, List

import pandas as pd
import polars as pl
from loguru import logger
from sqlalchemy import asc

from search import models, constant
from search.core.search_context import SearchContext


class CSVExportCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        pass

    @abstractmethod
    def valid_file(self, search_context: SearchContext) -> bool:
        pass


class DefaultCSVExportCache(CSVExportCache):

    def valid_file(self, search_context: SearchContext) -> bool:
        search_file_list: List[models.SearchFile] = (
            models.SearchFile
            .query
            .filter_by(search_md5=search_context.search_key,
                       use=constant.SEARCH,
                       status=constant.FileStatus.USABLE)
            .order_by(asc(models.SearchFile.order))
            .all())
        if len(search_file_list) == 0:
            return False

        res = [search_file and os.path.isfile(search_file.path) for search_file in search_file_list]
        return all(res)

    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        search_file_list: List[models.SearchFile] = (
            models.SearchFile
            .query
            .filter_by(search_md5=search_context.search_key,
                       use=constant.SEARCH,
                       status=constant.FileStatus.USABLE)
            .order_by(asc(models.SearchFile.order))
            .all())
        res = [search_file and os.path.isfile(search_file.path) for search_file in search_file_list]
        if all(res):
            df_list: List[pd.DataFrame] = []
            for search_file in search_file_list:
                df_list.append(pl.read_parquet(search_file.path).to_pandas(use_pyarrow_extension_array=True))
            if len(df_list) > 0:
                return pd.concat(df_list)
        else:
            logger.warning(f"{search_context.search_key}-{len(search_file_list)}无法查询到缓存文件")
