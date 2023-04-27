# @Time    : 2023/03/11 15:03
# @Author  : fyq
# @File    : __init__.py.py
# @Software: PyCharm

__author__ = 'fyq'

import simplejson as json
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

from pandas._libs import NaTType


class SearchEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            if isinstance(obj, NaTType):
                return ""
            else:
                return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, date):
            if isinstance(obj, NaTType):
                return ""
            else:
                return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, Decimal):
            return float(str(obj))
        elif isinstance(obj, UUID):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)
