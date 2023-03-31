# @Time    : 2023/03/10 8:53
# @Author  : fyq
# @File    : __init__.py.py
# @Software: PyCharm

__author__ = 'fyq'

import os

from .file_status import FileStatus
from .file_type import FileType
from .file_use import FileUse
from .search_compose import SearchCompose
from .search_status import SearchStatus
from .search_point import SearchPoint
from .redis_key_suffix import RedisKeySuffix

SEARCH_FIELD = "searchField"
SEARCH_CONDITION = "searchCondition"
EXPORT = "export"
SEARCH = "search"
SEARCH_NAME = "searchName"
DEFAULT_FILE_DIR = f"D:{os.path.sep}search_file"
PAGE_NUMBER = "pageNumber"
SEARCH_CONTEXT = "searchContext"
TOTAL = "total"
CSV = "csv"
PARAM = "param"
PROGRESS = "progress"
SEARCH_MD5 = "search_md5"
