# @Time    : 2023/05/06 11:52
# @Author  : fyq
# @File    : search_weight.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import Tuple
from abc import ABCMeta, abstractmethod


class ISearchWeight(metaclass=ABCMeta):

    @abstractmethod
    def weight(self, days: int, field_count: int, condition_count: int) -> Tuple[int, str]:
        pass


class AbstractSearchWeight(ISearchWeight):

    def __init__(self):
        self._weight = {
            "l": (8, 9),
            "m": (6, 7),
            "s": (4, 5, 6),
            "ss": (0, 1, 2)
        }

    def weight(self, days: int, field_count: int, condition_count: int) -> Tuple[int, str]:
        score = self.compute_days(days) + \
                self.compute_field_count(field_count) + \
                self.compute_condition_count(condition_count)
        weight = "ss"
        for k, v in self._weight.items():
            if score in v:
                weight = k
                break

        return score, weight

    @abstractmethod
    def compute_days(self, days: int) -> int:
        pass

    @abstractmethod
    def compute_field_count(self, field_count) -> int:
        pass

    @abstractmethod
    def compute_condition_count(self, condition_count) -> int:
        pass


class SearchWeight(AbstractSearchWeight):

    def compute_days(self, days: int) -> int:
        if days > 180:
            return 3
        elif days > 90:
            return 2
        elif days > 30:
            return 1
        else:
            return 0

    def compute_field_count(self, field_count) -> int:
        if field_count > 20:
            return 3
        elif field_count > 15:
            return 2
        elif field_count > 10:
            return 1
        else:
            return 0

    def compute_condition_count(self, condition_count) -> int:
        if condition_count < 3:
            return 3
        elif condition_count < 5:
            return 2
        elif condition_count < 7:
            return 1
        else:
            return 0
