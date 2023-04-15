# @Time    : 23/03/02 9:14
# @Author  : fyq
# @File    : search.py
# @Software: PyCharm

__author__ = 'fyq'

import os
from abc import ABCMeta, abstractmethod

import redis
import simplejson as json
from flask import current_app, Flask, make_response, send_file, Response
from loguru import logger

from search import constant, models
from search.core.cache import *
from search.core.progress import progress_manager
from search.core.search_context import SearchContext, scm
from search.core.search_local import search_local
from search.core.search_md5 import create_search_md5, SearchMd5
from search.entity import CommonResult, MessageCode
from search.exceptions import FileNotFindSearchException
from search.extend import thread_pool, redis_pool


class ISearch(metaclass=ABCMeta):

    @abstractmethod
    def search(self, data: str) -> dict:
        pass

    @abstractmethod
    def export(self, data: str) -> Response:
        pass

    @abstractmethod
    def export_progress(self, data: str) -> dict:
        pass

    @abstractmethod
    def search_progress(self, data: str) -> dict:
        pass


class Search(ISearch):

    def __init__(self):
        # csv导出
        self._csv_export_cache = DefaultCSVExportCache()
        # csv查询
        self._csv_search_cache = DefaultCSVSearchCache()
        # redis查询
        self._redis_search_cache = DefaultRedisSearchCache()
        # tar导出
        self._tar_export_cache = DefaultTarExportCache()
        # db查询
        self._db_search_cache = DefaultDBSearchCache()
        # db导出
        self._db_export_cache = DefaultDBExportCache()

    def search(self, data: str) -> dict:
        """
            {
                search: {
                    searchName: 'xx',
                    searchCondition: {
                        condition1: 'xx',
                        condition2: 'xx'
                    },
                    searchField: ['field1', field2]
                }
                param: {
                    pageNumber: 2,
                    cache: 0,
                }
            }
        :param data:
        :return:
        """
        data = json.loads(data)
        search_node = data.get(constant.SEARCH)
        param = data.get(constant.PARAM)
        page_number = param.get(constant.PAGE_NUMBER)
        search_md5: SearchMd5 = create_search_md5(search_node)
        search_context: SearchContext = scm.get_search_context(search_md5)
        data, page = self._redis_search_cache.get_data(search_context=search_context, page_number=page_number)
        if data is None:
            try:
                data, page = self._csv_search_cache.get_data(search_context=search_context, page_number=page_number)
                if data is None:
                    return CommonResult.fail(code=MessageCode.LAST_PAGE.code, message=MessageCode.LAST_PAGE.desc)
            except FileNotFindSearchException:
                if page_number == 1:
                    data_df = self._db_search_cache.get_data(search_context=search_context, top=True)
                    if len(data_df) > 0:
                        data_df_real_len = (len(
                            data_df) // search_context.search.page_size) * search_context.search.page_size
                        data_df = data_df.iloc[0:data_df_real_len]
                        self._redis_search_cache.set_data(search_context=search_context,
                                                          data_df=data_df,
                                                          page_begin=1,
                                                          whole=False)
                        data, page = self._redis_search_cache.get_data(search_context=search_context,
                                                                       page_number=page_number)
                    else:
                        return CommonResult.fail(code=MessageCode.LAST_PAGE.code, message=MessageCode.LAST_PAGE.desc)
                r = redis.Redis(connection_pool=redis_pool)
                if r.setnx(name=f"{search_context.search_key}_{constant.RedisKeySuffix.SEARCH}", value=1):
                    progress_manager.set_new_progress_step(constant.SEARCH, search_context.search_key)
                    search_context.search_future = thread_pool.submit(self._search_thread_func,
                                                                      current_app._get_current_object(),
                                                                      search_context)
        if data is None:
            return CommonResult.fail(code=MessageCode.NOT_READY.code, message=MessageCode.NOT_READY.desc)
        else:
            return CommonResult.success(data={"list": data, "page": page})

    def export(self, data: str) -> Response:
        """
            search: {
                searchName: 'xx',
                searchCondition: {
                    condition1: 'xx',
                    condition2: 'xx'
                },
                searchField: ['field1', field2]
            }

            导出的文件信息
        :param data:
        :return:
        """
        m = json.loads(data)
        search_md5: SearchMd5 = create_search_md5(m)
        search_context: SearchContext = scm.get_search_context(search_md5)
        search_file = self._tar_export_cache.get_data(search_context=search_context)
        if search_file and os.path.isfile(search_file.path):
            return make_response(send_file(path_or_file=search_file.path,
                                           as_attachment=True))
        else:
            r = redis.Redis(connection_pool=redis_pool)
            if r.setnx(name=f"{search_context.search_key}_{constant.RedisKeySuffix.EXPORT}", value=1):
                progress_manager.set_new_progress_step(constant.EXPORT, search_context.search_key)
                search_context.export_future = thread_pool.submit(self._export_thread_func,
                                                                  current_app._get_current_object(),
                                                                  search_context)

            response = make_response()
            response.status_code = 250
            return response

    def _search_thread_func(self, app: Flask, search_context: SearchContext):
        try:
            with app.app_context():
                with search_local(key=constant.SEARCH_MD5, value=search_context.search_key):
                    data_df = self._db_search_cache.get_data(search_context=search_context,
                                                             top=False)
                    if len(data_df) > search_context.search.want_csv:
                        file_dir = app.config.setdefault("FILE_DIR", constant.DEFAULT_FILE_DIR)
                        self._csv_search_cache.set_data(search_context=search_context,
                                                        file_dir=file_dir,
                                                        data_df=data_df)
                    else:
                        self._redis_search_cache.set_data(search_context=search_context,
                                                          data_df=data_df,
                                                          page_begin=1,
                                                          whole=True)
        except Exception as e:
            logger.exception(e)
        finally:
            r = redis.Redis(connection_pool=redis_pool)
            r.delete(f"{search_context.search_key}_{constant.RedisKeySuffix.SEARCH}")

    def _export_thread_func(self, app: Flask, search_context: SearchContext):
        try:
            with app.app_context():
                with search_local(key=constant.SEARCH_MD5, value=search_context.search_key):
                    data_df = self._csv_export_cache.get_data(search_context=search_context)
                    if data_df is None:
                        data_df = self._db_export_cache.get_data(search_context=search_context,
                                                                 top=False)
                    file_dir = app.config.setdefault("FILE_DIR", constant.DEFAULT_FILE_DIR)
                    self._tar_export_cache.set_data(search_context=search_context,
                                                    data_df=data_df,
                                                    file_dir=file_dir)
        except Exception as e:
            logger.exception(e)
        finally:
            r = redis.Redis(connection_pool=redis_pool)
            r.delete(f"{search_context.search_key}_{constant.RedisKeySuffix.EXPORT}")

    def export_progress(self, data: str) -> dict:
        data = json.loads(data)
        search_md5: SearchMd5 = create_search_md5(data)
        search: models.Search = models.Search.query.filter_by(name=search_md5.search_name).first()
        if not search:
            return CommonResult.fail(code=MessageCode.NOT_PROGRESS.code,
                                     message=MessageCode.NOT_PROGRESS.desc)

        mid = f"{search.name}_v{search.version}_{search_md5.search_md5}"
        logger.info(f"search progress for {constant.EXPORT}_{mid}")

        progress_step = \
            progress_manager.find_progress_step(constant.EXPORT,
                                                mid)
        if progress_step:
            return CommonResult.success(data=progress_step.info)
        else:
            return CommonResult.fail(code=MessageCode.NOT_PROGRESS.code,
                                     message=MessageCode.NOT_PROGRESS.desc)

    def search_progress(self, data: str) -> dict:
        data = json.loads(data)
        search_md5: SearchMd5 = create_search_md5(data)
        search: models.Search = models.Search.query.filter_by(name=search_md5.search_name).first()
        if not search:
            return CommonResult.fail(code=MessageCode.NOT_PROGRESS.code,
                                     message=MessageCode.NOT_PROGRESS.desc)

        mid = f"{search.name}_v{search.version}_{search_md5.search_md5}"
        logger.info(f"search progress for {constant.SEARCH}_{mid}")

        progress_step = \
            progress_manager.find_progress_step(constant.SEARCH,
                                                mid)
        if progress_step:
            return CommonResult.success(data=progress_step.info)
        else:
            return CommonResult.fail(code=MessageCode.NOT_PROGRESS.code,
                                     message=MessageCode.NOT_PROGRESS.desc)


current_search = Search()
