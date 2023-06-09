# @Time    : 2023/03/12 20:45
# @Author  : fyq
# @File    : progress.py
# @Software: PyCharm

__author__ = 'fyq'

import functools
import math
import time
import uuid
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import List, Dict, Tuple, Optional

import redis
from loguru import logger

from search import constant, models, db
from search.core.search_context import SearchContext
from search.core.search_local import search_local
from search.exceptions import ProgressException

import simplejson

from search.extend import redis_pool


@dataclass
class ProgressInfo:
    steps: List[int] = field(default_factory=lambda: [])

    count: int = 0

    records: List[models.SearchRecord] = field(default_factory=lambda: [])

    @property
    def value(self):
        return math.floor(len(self.steps) * 100 / self.count)


class ProgressStep(metaclass=ABCMeta):

    @abstractmethod
    def _steps(self) -> List[Tuple[str, str]]:
        pass

    def __init__(self):
        self._progress_infos: Dict[str, ProgressInfo] = {}
        self.error = ""

    def get_progress_info(self, step_name) -> ProgressInfo:
        steps = [s[0] for s in self._steps()]
        if step_name not in steps:
            raise ProgressException(f"步骤[{step_name}]不存在")
        for s in steps[0: steps.index(step_name)]:
            if s in self._progress_infos:
                p = self._progress_infos[s]
                if p.value != 100:
                    p.count = 1
                    p.steps = [1]
            else:
                p = ProgressInfo()
                p.count = 1
                p.steps = [1]
                self._progress_infos[s] = p
        return self._progress_infos.setdefault(step_name, ProgressInfo())

    def last_step(self, step_name: str) -> bool:
        steps = [s[0] for s in self._steps()]
        if step_name not in steps:
            raise ProgressException(f"步骤[{step_name}]不存在")
        return steps[-1] == step_name

    @property
    def info(self):

        _progress: List = []
        for step in self._steps():
            _value = 0
            if step[0] in self._progress_infos:
                _value = self._progress_infos[step[0]].value
            _progress.append({
                "name": step[0],
                "display": step[1],
                "value": _value
            })

        if len(_progress):
            if all([p["value"] == 0 for p in _progress]):
                _progress[0]["value"] = 1

        return {
            "error": self.error,
            "progress": _progress
        }

    def to_db(self):
        record_id = uuid.uuid4()
        for p, v in self._progress_infos.items():
            for r in v.records:
                r.record_id = record_id
            db.session.add_all(v.records)
        db.session.commit()


class SearchProgressStep(ProgressStep):

    def _steps(self) -> List[Tuple[str, str]]:
        return [("db", "数据查询中"), ("redis_o_file", "数据打包中")]


class ExportProgressStep(ProgressStep):

    def _steps(self) -> List[Tuple[str, str]]:
        return [("db", "数据查询中"), ("tar", "数据打包中")]


class Progress:

    def __init__(self, prefix: str = "", suffix: str = ""):
        self._suffix = suffix
        self._prefix = prefix

    def __call__(self, wrapper_cls):

        execs = getattr(wrapper_cls, "execs", [])
        count_func = getattr(wrapper_cls, "count")
        if count_func is None or not callable(count_func):
            raise ProgressException(f"未找到count或者count必须是可调用的")
        setattr(wrapper_cls, "count", self._count(count_func))
        for e in execs:
            exec_func = getattr(wrapper_cls, e)
            setattr(wrapper_cls, e, self._exec(exec_func))

        return wrapper_cls

    def _count(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            search_md5 = search_local.get_value(constant.SEARCH_MD5)
            try:
                res = f(*args, **kwargs)
                if search_md5:
                    progress_info = progress_manager.get_progress_info(self._prefix, search_md5, self._suffix)
                    progress_info.count = res
                return res
            except Exception as e:
                if search_md5:
                    progress_step = progress_manager.find_progress_step(self._prefix, search_md5)
                    if progress_step:
                        progress_step.error = str(e)
                raise e

        return wrapper

    def _exec(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            search_md5 = search_local.get_value(constant.SEARCH_MD5)
            try:
                begin_time = time.perf_counter()
                res = f(*args, **kwargs)
                end_time = round(time.perf_counter() - begin_time, 3)
                if search_md5:
                    progress_step = progress_manager.get_progress_step(self._prefix, search_md5)
                    progress_info = progress_step.get_progress_info(self._suffix)
                    new_step_no = len(progress_info.steps) + 1
                    progress_info.steps.append(new_step_no)
                    logger.info(f"[{self._prefix}_{search_md5}_{self._suffix}]进度:{progress_info.value},耗时:{end_time}s")
                    search_context: SearchContext = kwargs.get("search_context", None)
                    if search_context:
                        sql: str = kwargs.get("sql")
                        search_record: models.SearchRecord = models.SearchRecord()
                        search_record.search_key = search_context.search_key
                        search_record.search_id = search_context.search.id
                        search_record.search_json = simplejson.dumps(search_context.search_md5.search)
                        search_record.search_time = end_time
                        search_record.order = new_step_no
                        search_record.search_suffix = self._suffix
                        search_record.search_prefix = self._prefix
                        if sql:
                            search_record.memo = sql
                        progress_info.records.append(search_record)

                        r = redis.Redis(connection_pool=redis_pool)
                        r.setex(name=f"{constant.RedisKey.PROGRESS_LOCK_PREFIX}_{self._prefix}_{search_md5}",
                                value=simplejson.dumps(progress_step.info),
                                time=43200)

                        if progress_step.last_step(self._suffix) and progress_info.value == 100:
                            progress_step.to_db()
                            progress_manager.delete_progress_step(self._prefix, search_md5)
                return res
            except Exception as e:
                if search_md5:
                    progress_step = progress_manager.find_progress_step(self._prefix, search_md5)
                    if progress_step:
                        progress_step.error = str(e)
                raise e

        return wrapper


class ProgressManager:

    def __init__(self):
        self._progresses: Dict[str, Dict[str, ProgressStep]] = {}

    def get_progress_info(self, prefix: str, search_md5: str, suffix: str) -> ProgressInfo:
        progress_steps = self._progresses.setdefault(prefix, dict())
        if search_md5 not in progress_steps:
            progress_steps[search_md5] = self._create_progress_step(prefix, search_md5)
        progress_step = progress_steps[search_md5]
        return progress_step.get_progress_info(suffix)

    def get_progress_step(self, prefix: str, search_md5: str) -> ProgressStep:
        progress_steps = self._progresses.setdefault(prefix, dict())
        if search_md5 not in progress_steps:
            progress_steps[search_md5] = self._create_progress_step(prefix, search_md5)
        return progress_steps[search_md5]

    def set_new_progress_step(self, prefix: str, search_md5: str):
        self._progresses.setdefault(prefix, dict())[search_md5] = self._create_progress_step(prefix, search_md5)

    def find_progress_step(self, prefix: str, search_md5: str) -> Optional[ProgressStep]:
        if prefix in self._progresses:
            if search_md5 in self._progresses[prefix]:
                return self._progresses[prefix][search_md5]

    def delete_progress_step(self, prefix: str, search_md5: str):
        if prefix in self._progresses:
            if search_md5 in self._progresses[prefix]:
                self._progresses[prefix].pop(search_md5)

    @classmethod
    def get_progress_step_info(cls, prefix: str, search_md5: str) -> Optional[Dict]:
        r = redis.Redis(connection_pool=redis_pool)
        value: bytes = r.get(name=f"{constant.RedisKey.PROGRESS_LOCK_PREFIX}_{prefix}_{search_md5}")
        if value:
            return simplejson.loads(value.decode())
        else:
            return None

    @staticmethod
    def _create_progress_step(prefix: str, search_md5: str) -> Optional[ProgressStep]:
        if prefix == constant.SEARCH:
            sp = SearchProgressStep()
            r = redis.Redis(connection_pool=redis_pool)
            r.setex(name=f"{constant.RedisKey.PROGRESS_LOCK_PREFIX}_{prefix}_{search_md5}",
                    value=simplejson.dumps(sp.info),
                    time=43200)
            return sp

        else:
            ep = ExportProgressStep()
            r = redis.Redis(connection_pool=redis_pool)
            r.setex(name=f"{constant.RedisKey.PROGRESS_LOCK_PREFIX}_{prefix}_{search_md5}",
                    value=simplejson.dumps(ep.info),
                    time=43200)
            return ep


progress_manager = ProgressManager()
