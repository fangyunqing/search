# @Time    : 2023/03/21 11:46
# @Author  : fyq
# @File    : config.py
# @Software: PyCharm

__author__ = 'fyq'

from flask import Blueprint, request

from search.core.config.datasource import datasource_config
from search.core.config.search import search_config

config_bp = Blueprint("config", __name__)


@config_bp.route(rule="/datasource", methods=["GET"])
def get_search_datasource():
    return datasource_config.get_datasource()


@config_bp.route(rule="/datasource/usable/<int:datasource_id>", methods=["GET"])
def datasource_usable(datasource_id):
    return datasource_config.usable(datasource_id)


@config_bp.route(rule="/datasource/disable/<int:datasource_id>", methods=["GET"])
def datasource_disable(datasource_id):
    return datasource_config.disable(datasource_id)


@config_bp.route(rule="/datasource/modify", methods=["POST"])
def datasource_modify():
    return datasource_config.modify(request.data.decode())


@config_bp.route(rule="/datasource/add", methods=["POST"])
def datasource_add():
    return datasource_config.add(request.data.decode())


@config_bp.route(rule="/search", methods=["GET"])
def get_search():
    return search_config.search(request.args)


@config_bp.route(rule="/search/condition/<int:search_id>", methods=["GET"])
def get_search_condition(search_id):
    return search_config.condition(search_id)


@config_bp.route(rule="/search/sql/<int:search_id>", methods=["GET"])
def get_search_sql(search_id):
    return search_config.sql(search_id)


@config_bp.route(rule="/search/field/<int:search_id>", methods=["GET"])
def get_search_field(search_id):
    return search_config.field(search_id)


@config_bp.route(rule="/search/sort/<int:search_id>", methods=["GET"])
def get_search_sort(search_id):
    return search_config.sort(search_id)


@config_bp.route(rule="/search/add", methods=["POST"])
def search_add():
    return search_config.add(request.data.decode())


@config_bp.route(rule="/search/modify", methods=["POST"])
def search_modify():
    return search_config.modify(request.data.decode())


@config_bp.route(rule="/search/parse/<int:version>/<int:search_id>", methods=["GET"])
def search_parse(search_id, version):
    return search_config.parse(search_id, version)


@config_bp.route(rule="/search/info/<int:search_id>")
def search_info(search_id):
    return search_config.info(search_id)


@config_bp.route(rule="/search/usable/<int:version>/<int:search_id>", methods=["GET"])
def search_usable(search_id, version):
    return search_config.usable(search_id, version)


@config_bp.route(rule="/search/disable/<int:version>/<int:search_id>", methods=["GET"])
def search_disable(search_id, version):
    return search_config.disable(search_id, version)


@config_bp.route(rule="/search/parameter", methods=["GET"])
def search_parameter():
    return search_config.search_parameter(request.args)


@config_bp.route(rule="/search/parameter/modify", methods=["POST"])
def modify_search_parameter():
    return search_config.modify_search_parameter(request.data.decode())
