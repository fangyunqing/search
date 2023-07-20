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
from munch import Munch

from search import constant, models
from search.core.cache import *
from search.core.notice import export_tar_notice
from search.core.progress import progress_manager
from search.core.search_context import SearchContext, scm
from search.core.search_local import search_local
from search.core.search_md5 import create_search_md5, SearchMd5
from search.core.search_param import sph
from search.core.strategy import search_strategy
from search.entity import CommonResult, MessageCode
from search.exceptions import FileNotFindSearchException, SearchStrategyException
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

    @abstractmethod
    def test_search(self, data: str) -> dict:
        pass


class Search(ISearch):

    def __init__(self):
        # csv导出
        self._csv_export_cache = DefaultCSVExportCache()
        # csv查询
        self._parquet_search_cache = DefaultParquetSearchCache()
        # redis查询
        self._redis_search_cache = DefaultRedisSearchCache()
        # tar导出
        self._tar_export_cache = DefaultTarExportCache()
        # db查询
        self._db_search_cache = DefaultDBSearchPolarsCache()
        # db导出
        self._db_export_cache = DefaultDBExportPolarsCache()

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
                params: {
                    pageNumber: 2,
                    cache: 0,
                }
            }
        :params data:
        :return:
        """
        data = json.loads(data)
        search_node = data.get(constant.SEARCH)
        params = Munch(data.get(constant.PARAM))
        page_number = params.get(constant.PAGE_NUMBER)
        search_md5: SearchMd5 = create_search_md5(search_node)
        search_context: SearchContext = scm.get_search_context(search_md5)
        sph.handle(params=params, search_context=search_context)
        data, page = self._redis_search_cache.get_data(search_context=search_context, page_number=page_number)
        if data is None:
            try:
                data, page = self._parquet_search_cache.get_data(search_context=search_context, page_number=page_number)
                if data is None:
                    return CommonResult.fail(code=MessageCode.LAST_PAGE.code, message=MessageCode.LAST_PAGE.desc)
            except FileNotFindSearchException:
                if page_number == 1:
                    data_df = self._db_search_cache.get_data(search_context=search_context, top=True, params=params)
                    if len(data_df) > 0:
                        data_df_real_len = (len(
                            data_df) // search_context.search.page_size) * search_context.search.page_size
                        data_df = data_df.slice(0, data_df_real_len)
                        self._redis_search_cache.set_data(search_context=search_context,
                                                          data_df=data_df,
                                                          page_begin=1,
                                                          whole=False)
                        data, page = self._redis_search_cache.get_data(search_context=search_context,
                                                                       page_number=page_number)
                    else:
                        return CommonResult.fail(code=MessageCode.LAST_PAGE.code, message=MessageCode.LAST_PAGE.desc)

                # 判断是否可以查询
                if data is None:
                    if not search_strategy.can_search(search_context=search_context):
                        raise SearchStrategyException()

                r = redis.Redis(connection_pool=redis_pool)
                thread_key = f"{constant.RedisKey.THREAD_LOCK_PREFIX}" \
                             f"_{constant.RedisKey.SEARCH}_{search_context.search_key}"
                if r.set(name=thread_key,
                         value=1,
                         nx=True,
                         ex=7200):
                    progress_manager.set_new_progress_step(constant.SEARCH, search_context.search_key)
                    thread_pool.submit(self._search_thread_func,
                                       current_app._get_current_object(),
                                       search_context,
                                       params) \
                        .add_done_callback(lambda f: r.delete(thread_key))
        if data is None:
            return CommonResult.fail(code=MessageCode.NOT_READY.code, message=MessageCode.NOT_READY.desc)
        else:
            return CommonResult.success(data={"list": data, "page": page})

    def test_search(self, data: str) -> dict:
        m = json.loads(data)
        search_md5: SearchMd5 = create_search_md5(m)
        search_context: SearchContext = scm.get_search_context(search_md5)
        self._db_export_cache.get_data(search_context=search_context,
                                       top=False,
                                       params=Munch())
        return CommonResult.success()

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
        data = json.loads(data)
        m = data.get(constant.SEARCH)
        params = Munch(data.get(constant.PARAM))
        search_md5: SearchMd5 = create_search_md5(m)
        search_context: SearchContext = scm.get_search_context(search_md5)
        search_file = self._tar_export_cache.get_data(search_context=search_context)
        sph.handle(params=params, search_context=search_context)
        if search_file and os.path.isfile(search_file.path):
            return make_response(send_file(path_or_file=search_file.path,
                                           as_attachment=True))
        else:
            if not self._csv_export_cache.valid_file(search_context=search_context):
                if not search_strategy.can_search(search_context=search_context):
                    response = make_response()
                    response.status_code = 601
                    return response
            r = redis.Redis(connection_pool=redis_pool)
            thread_key = f"{constant.RedisKey.THREAD_LOCK_PREFIX}" \
                         f"_{constant.RedisKey.EXPORT}_{search_context.search_key}"
            if r.set(name=thread_key,
                     value=1,
                     nx=True,
                     ex=7200,
                     ):
                progress_manager.set_new_progress_step(constant.EXPORT, search_context.search_key)
                thread_pool.submit(self._export_thread_func,
                                   current_app._get_current_object(),
                                   search_context,
                                   params) \
                    .add_done_callback(lambda f: (r.delete(thread_key), export_tar_notice.notice(search_context)))

            response = make_response()
            response.status_code = 250
            return response

    def _search_thread_func(self, app: Flask, search_context: SearchContext, params: Munch):
        try:
            with app.app_context():
                with search_local(key=constant.SEARCH_MD5, value=search_context.search_key):
                    data_df = self._db_search_cache.get_data(search_context=search_context,
                                                             top=False,
                                                             params=params)
                    if len(data_df) > search_context.search.file_cache_limit:
                        file_dir = app.config.setdefault("FILE_DIR", constant.DEFAULT_FILE_DIR)
                        self._parquet_search_cache.set_data(search_context=search_context,
                                                            file_dir=file_dir,
                                                            data_df=data_df)
                    else:
                        self._redis_search_cache.set_data(search_context=search_context,
                                                          data_df=data_df,
                                                          page_begin=1,
                                                          whole=True)
        except Exception as e:
            logger.exception(e)

    def _export_thread_func(self, app: Flask, search_context: SearchContext, params: Munch):
        try:
            with app.app_context():
                with search_local(key=constant.SEARCH_MD5, value=search_context.search_key):
                    data_df = self._csv_export_cache.get_data(search_context=search_context)
                    if data_df is None:
                        data_df = self._db_export_cache.get_data(search_context=search_context,
                                                                 top=False,
                                                                 params=params)
                        data_df = data_df.to_pandas(use_pyarrow_extension_array=True)
                    file_dir = app.config.setdefault("FILE_DIR", constant.DEFAULT_FILE_DIR)
                    self._tar_export_cache.set_data(search_context=search_context,
                                                    data_df=data_df,
                                                    file_dir=file_dir)
        except Exception as e:
            logger.exception(e)

    def export_progress(self, data: str) -> dict:
        data = json.loads(data)
        m = data.get(constant.SEARCH)
        search_md5: SearchMd5 = create_search_md5(m)
        search: models.Search = models.Search.query.filter_by(name=search_md5.search_name).first()
        if not search:
            return CommonResult.fail(code=MessageCode.NOT_PROGRESS.code,
                                     message=MessageCode.NOT_PROGRESS.desc)

        mid = f"{search.name}_v{search.version}_{search_md5.search_md5}"
        logger.info(f"search progress for [{constant.EXPORT}_{mid}]")

        progress_step_info = progress_manager.get_progress_step_info(constant.EXPORT, mid)
        if progress_step_info:
            return CommonResult.success(data=progress_step_info)
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
        logger.info(f"search progress for [{constant.SEARCH}_{mid}]")

        progress_step_info = progress_manager.get_progress_step_info(constant.SEARCH, mid)
        if progress_step_info:
            return CommonResult.success(data=progress_step_info)
        else:
            return CommonResult.fail(code=MessageCode.NOT_PROGRESS.code,
                                     message=MessageCode.NOT_PROGRESS.desc)


current_search = Search()
