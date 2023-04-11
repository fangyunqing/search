# @Time    : 2023/03/31 11:52
# @Author  : fyq
# @File    : test.py
# @Software: PyCharm

__author__ = 'fyq'

from flask import Blueprint

from search.core.search_context import scm

test_bp = Blueprint("test", __name__)


@test_bp.route(rule="/connect")
def test():
    return "ok"
