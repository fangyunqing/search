# @Time    : 2023/03/20 9:49
# @Author  : fyq
# @File    : file_timer.py
# @Software: PyCharm

__author__ = 'fyq'

import datetime
import os
from typing import List

import redis

from search import models
from search.extend import scheduler, db, redis_pool
from search import constant
from loguru import logger


@scheduler.task('cron', id='delete_file', minute="*/30")
def delete_file():
    r = redis.Redis(connection_pool=redis_pool)
    if r.setnx(constant.RedisKeySuffix.SEARCH_DELETE_FILE, value=1):
        try:
            with scheduler.app.app_context():
                try:
                    # 先删除悬挂的文件
                    search_file_mountings: List[models.SearchFile] = \
                        models.SearchFile.query.filter_by(status=constant.FileStatus.MOUNTING).all()
                    for search_file in search_file_mountings:
                        try:
                            if os.path.isfile(search_file.path):
                                os.remove(search_file.path, dir_fd=None)
                                if search_file.use == constant.FileUse.EXPORT:
                                    # export会创建目录 目录也删除
                                    d, f = os.path.split(search_file.path)
                                    if os.path.isdir(d):
                                        os.rmdir(d)
                            search_file.status = constant.FileStatus.DELETE
                            logger.info(f"删除文件ID[{search_file.id}]")
                        except Exception as e:
                            logger.warning(e)

                    # 在用变成悬挂
                    delete_redis_keys = []
                    search_file_usable_list: List[models.SearchFile] = \
                        models.SearchFile.query.filter_by(status=constant.FileStatus.USABLE).all()
                    now = datetime.datetime.now()
                    for search_file in search_file_usable_list:
                        search: models.Search = models.Search.query.filter_by(id=search_file.search_id).first()
                        if search is None:
                            logger.warning(f"文件ID[{search_file.id}]未找到搜索")
                            expire_time = search_file.create_time
                        else:
                            expire_time = \
                                search_file.create_time + datetime.timedelta(minutes=search.export_file_cache_time)
                        if expire_time < now:
                            search_file.status = constant.FileStatus.MOUNTING
                            logger.info(f"悬挂文件ID[{search_file.id}]")
                            delete_redis_keys.append(f"{search_file.search_md5}_{constant.CSV}")

                    # 提交
                    db.session.commit()
                    # 删除redis锁
                    if len(delete_redis_keys) > 0:
                        r.delete(*delete_redis_keys)
                except Exception as e:
                    logger.exception(e)
        finally:
            r.delete(constant.RedisKeySuffix.SEARCH_DELETE_FILE)









