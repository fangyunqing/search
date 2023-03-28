# @Time    : 23/02/23 13:22
# @Author  : fyq
# @File    : search_expression.py
# @Software: PyCharm

__author__ = 'fyq'

from dataclasses import dataclass, field

from search.models import SearchSQL
from typing import List, Dict, Set


@dataclass
class SearchExpression:

    search_sql: SearchSQL = None
    sql_field: Set[str] = field(default_factory=lambda: set())
    sql_select: Set[str] = field(default_factory=lambda: set())





