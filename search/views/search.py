# @Time    : 23/02/21 15:33
# @Author  : fyq
# @File    : search.py
# @Software: PyCharm

__author__ = 'fyq'

from typing import List

import munch
from flask import Blueprint, request
from loguru import logger

from search import models, dm
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
        models.SearchCondition.query.filter_by(search_id=search_result.id).order_by(
            models.SearchCondition.order).all()

    search_condition_list: List[munch.Munch] = [munch.Munch(s.to_dict()) for s in search_condition_list]

    conn = dm.get_main_connection()
    if conn:
        cur = conn.cursor()
        try:
            for search_condition in search_condition_list:
                if search_condition.datatype in ["list", "lookup"] and \
                        search_condition.list_values and \
                        len(search_condition.list_values) > 0:
                    try:
                        cur.execute(search_condition.list_values)
                        datas = cur.fetchall()
                        search_condition.list_values = []
                        if search_condition.datatype == "lookup":
                            search_condition.lookup_values = []
                            for data in datas:
                                key = None
                                value = None
                                if len(data) > 0:
                                    key = str(data[0])
                                if len(data) > 1:
                                    value = str(data[1])
                                if key and value:
                                    search_condition.lookup_values.append({
                                        "key": key,
                                        "value": value
                                    })
                        else:
                            for data in datas:
                                if len(data) > 0:
                                    search_condition.list_values.append(str(data[0]))
                    except Exception as e:
                        logger.exception(e)
        finally:
            cur.close()
            conn.close()
    else:
        for search_condition in search_condition_list:
            if search_condition.datatype in ["list", "lookup"]:
                search_condition.list_values = []
    return CommonResult.success(data=search_condition_list)


@search_bp.route(rule="/field", methods=["GET"])
def field():
    search_name = request.args.get("searchName")
    search_result = models.Search.query.filter_by(name=search_name).first()
    search_field_list = \
        models.SearchField.query.filter_by(search_id=search_result.id).order_by(
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
