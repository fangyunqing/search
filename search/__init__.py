# @Time    : 23/02/20 9:08
# @Author  : fyq
# @File    : __init__.py
# @Software: PyCharm

__author__ = 'fyq'

from flask import Flask, request, Response, make_response
from flask.scaffold import setupmethod
from loguru import logger

from search.database import dm
from search.entity import CommonResult, MessageCode, BeforeRequest, AfterRequest
from search.extend import db, migrate, scheduler
from search.models import SearchDatasource
from search.views import search_bp, config_bp, user_bp, test_bp
import simplejson


def create_app():
    app = Flask(__name__)

    # 配置文件
    app.config.from_object("search.config.settings")
    db.init_app(app)
    # 初始化
    migrate.init_app(app, db)
    # 注册蓝图
    app.register_blueprint(search_bp, url_prefix="/search")
    app.register_blueprint(test_bp, url_prefix="/test")
    app.register_blueprint(config_bp, url_prefix="/config")
    app.register_blueprint(user_bp, url_prefix="/user")
    # 定时器
    scheduler.init_app(app)
    import search.core.timer as search_timer
    search_timer.start()

    @app.before_request
    def before_request():

        br = BeforeRequest()
        br.method = request.method
        br.path = request.path
        br.remote_addr = request.remote_addr
        br.args = simplejson.dumps(request.args)
        br.form = simplejson.dumps(request.form)
        br.data = simplejson.dumps(request.data.decode())

        logger.info(f"请求参数:{br.to_dict()}")

    @app.after_request
    def after_request(response: Response):

        ar = AfterRequest()
        ar.path = request.path
        json = simplejson.dumps(response.json)
        if len(json) < 1024:
            ar.json = json
        ar.content_length = response.content_length
        data = response.data.decode()
        if len(data) < 1024:
            ar.data = data

        logger.info(f"响应参数:{ar.to_dict()}")

        return response

    @app.before_first_request
    def init_datasource():
        for ds in SearchDatasource.query.filter_by(usable="1").all():
            dm.register(ds)

    @app.errorhandler(Exception)
    def handle_exception(e):
        logger.exception(e)
        response = make_response()
        response.data = simplejson.dumps(CommonResult.fail(MessageCode.ERROR.code, message=str(e)))
        response.content_type = "application/json"
        return response

    return app
