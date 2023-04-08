# @Time    : 2023/03/12 20:45
# @Author  : fyq
# @File    : progress.py
# @Software: PyCharm

__author__ = 'fyq'

import functools
import math
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional

from loguru import logger

from search import constant
from search.core.search_local import search_local
from search.exceptions import ProgressException


@dataclass
class ProgressInfo:
    steps: List[int] = field(default_factory=lambda: [])

    count: int = 0

    @property
    def value(self):
        return math.ceil(len(self.steps) * 100 / self.count)


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

        return {
            "error": self.error,
            "progress": _progress
        }


class SearchProgressStep(ProgressStep):

    def _steps(self) -> List[Tuple[str, str]]:
        return [("db", "数据查询中")]


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
                res = f(*args, **kwargs)
                if search_md5:
                    progress_info = progress_manager.get_progress_info(self._prefix, search_md5, self._suffix)
                    progress_info.steps.append(1)
                    logger.info(f"[{self._prefix}_{search_md5}_{self._suffix}]进度:{progress_info.value}")
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
        return self._progresses.setdefault(prefix, dict()) \
            .setdefault(search_md5, self._create_progress_step(prefix)) \
            .get_progress_info(suffix)

    def set_new_progress_step(self, prefix: str, search_md5: str):
        self._progresses.setdefault(prefix, dict())[search_md5] = self._create_progress_step(prefix)

    def find_progress_step(self, prefix: str, search_md5: str) -> Optional[ProgressStep]:
        if prefix in self._progresses:
            if search_md5 in self._progresses[prefix]:
                return self._progresses[prefix][search_md5]

    @staticmethod
    def _create_progress_step(prefix: str) -> Optional[ProgressStep]:
        if prefix == constant.SEARCH:
            return SearchProgressStep()
        else:
            return ExportProgressStep()


progress_manager = ProgressManager()
