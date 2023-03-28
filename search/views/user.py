# @Time    : 2023/03/21 14:05
# @Author  : fyq
# @File    : user.py
# @Software: PyCharm

__author__ = 'fyq'

from flask import Blueprint, request

from search.entity import CommonResult

user_bp = Blueprint("user", __name__)


@user_bp.route(rule="/login", methods=["POST"])
def login():
    return CommonResult.success(data={"token": "admin-token"})


@user_bp.route(rule="/info", methods=["GET"])
def info():
    ret = {
        "roles": ['admin'],
        "introduction": 'I am a super administrator',
        "avatar": 'https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif',
        "name": 'Super Admin'
    }
    return CommonResult.success(data=ret)
