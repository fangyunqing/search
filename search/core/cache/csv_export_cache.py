# @Time    : 2023/03/11 16:12
# @Author  : fyq
# @File    : csv_export_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import os
from abc import ABCMeta, abstractmethod
from typing import Optional, List

import pandas as pd
from loguru import logger
from sqlalchemy import asc

from search import models, constant
from search.core.search_context import SearchContext


class CSVExportCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        pass


class DefaultCSVExportCache(CSVExportCache):

    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        search_file_list: List[models.SearchFile] = \
            models.SearchFile.query.filter_by(search_md5=search_context.search_key,
                                              use=constant.SEARCH,
                                              status=constant.FileStatus.USABLE) \
                                   .order_by(asc(models.SearchFile.order)) \
                                   .all()
        res = [search_file and os.path.isfile(search_file.path) for search_file in search_file_list]
        if all(res):
            df_list: List[pd.DataFrame] = []
            for search_file in search_file_list:
                df_list.append(pd.read_csv(filepath_or_buffer=search_file.path, sep="`"))
            if len(df_list) > 0:
                return pd.concat(df_list)
        else:
            logger.warning(f"{search_context.search_key}-{len(search_file_list)}无法查询到缓存文件")
