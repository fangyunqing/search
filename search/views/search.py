# @Time    : 23/02/21 15:33
# @Author  : fyq
# @File    : search.py
# @Software: PyCharm

__author__ = 'fyq'

from flask import Blueprint, request

from search import models
from search.core import current_search

from search.entity import CommonResult

search_bp = Blueprint("search", __name__)


@search_bp.route(rule="/search", methods=["POST"])
def search():
    return current_search.search(request.data.decode())


@search_bp.route(rule="/condition", methods=["GET"])
def condition():
    search_name = request.args.get("searchName")
    search_result = models.Search.query.filter_by(name=search_name).first()
    search_condition_list = \
        models.SearchCondition.query.filter_by(search_id=search_result.id).filter_by(usable=1).order_by(
            models.SearchCondition.order).all()

    return CommonResult.success(data=[s.to_dict() for s in search_condition_list])


@search_bp.route(rule="/field", methods=["GET"])
def field():
    search_name = request.args.get("searchName")
    search_result = models.Search.query.filter_by(name=search_name).first()
    search_field_list = \
        models.SearchField.query.filter_by(search_id=search_result.id).filter_by(usable=1).order_by(
            models.SearchField.order).all()
    return CommonResult.success(data=[s.to_dict() for s in search_field_list])


@search_bp.route(rule="/export", methods=["POST"])
def export():
    return current_search.export(request.data.decode())


@search_bp.route(rule="/export_progress", methods=["POST"])
def export_progress():
    return current_search.export_progress(request.data.decode())


@search_bp.route(rule="/search_progress", methods=["POST"])
def search_progress():
    return current_search.search_progress(request.data.decode())
