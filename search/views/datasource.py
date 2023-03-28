# @Time    : 23/02/22 14:19
# @Author  : fyq
# @File    : datasource.py
# @Software: PyCharm

__author__ = 'fyq'

# 初始数据库
from search import SearchDatasource
from flask import Blueprint

ds_bp = Blueprint("ds", __name__)
