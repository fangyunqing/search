# @Time    : 2023/03/29 9:04
# @Author  : fyq
# @File    : search_md5.py
# @Software: PyCharm

__author__ = 'fyq'

import copy
import hashlib
from dataclasses import dataclass, field
from typing import List, Dict

from search import constant

import simplejson

from search.entity.common_result import BaseDataClass


@dataclass
class SearchMd5(BaseDataClass):
    search: Dict = None

    search_name: str = None

    search_md5: str = None

    search_field_md5: str = None

    search_condition_md5: str = None

    search_original_field_list: List[str] = field(default_factory=lambda: [])

    search_sort_field_list: List[str] = field(default_factory=lambda: [])

    search_conditions: Dict = None

    search_sort_condition_list: List[str] = field(default_factory=lambda: [])

    search_sort_condition_value_list: List[str] = field(default_factory=lambda: [])


def create_search_md5(search: Dict) -> SearchMd5:
    search_name = search.get(constant.SearchPoint.SEARCH_NAME, "")
    search_fields = search.get(constant.SearchPoint.SEARCH_FIELD, [])
    search_conditions = search.get(constant.SearchPoint.SEARCH_CONDITION, {})

    sm = SearchMd5()
    sm.search = search
    sm.search_name = search_name
    sm.search_original_field_list = search_fields
    sm.search_sort_field_list = copy.deepcopy(search_fields)
    sm.search_sort_field_list.sort()

    sm.search_conditions = {key: value for key, value in search_conditions.items() if
                            (isinstance(value, str) and len(value) > 0) or value}

    sm.search_sort_condition_list = list(sm.search_conditions)
    sm.search_sort_condition_list.sort()
    for condition in sm.search_sort_condition_list:
        sm.search_sort_condition_value_list.append(sm.search_conditions.get(condition))

    md5_list = [sm.search_name, sm.search_sort_field_list]
    sm.search_field_md5 = hashlib.md5(simplejson.dumps(md5_list).encode(encoding='utf-8')).hexdigest()
    md5_list.append(sm.search_sort_condition_list)
    sm.search_condition_md5 = hashlib.md5(simplejson.dumps(md5_list).encode(encoding='utf-8')).hexdigest()
    md5_list.append(sm.search_sort_condition_value_list)
    sm.search_md5 = hashlib.md5(simplejson.dumps(md5_list).encode(encoding='utf-8')).hexdigest()

    return sm
