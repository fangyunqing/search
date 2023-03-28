# @Time    : 23/02/20 9:08
# @Author  : fyq
# @File    : __init__.py
# @Software: PyCharm

__author__ = 'fyq'

from flask import Flask, request, Response, make_response
from flask.scaffold import setupmethod
from loguru import logger

from search.database import dm
from search.entity import CommonResult, MessageCode
from search.extend import db, migrate, scheduler
from search.models import SearchDatasource
from search.views import init_bp, search_bp, ds_bp, config_bp, user_bp
import simplejson


def create_app():
    app = Flask(__name__)

    # 配置文件
    app.config.from_object("search.config.settings")
    db.init_app(app)
    # 初始化
    migrate.init_app(app, db)
    # 注册蓝图
    app.register_blueprint(init_bp)
    app.register_blueprint(search_bp, url_prefix="/search")
    app.register_blueprint(ds_bp)
    app.register_blueprint(config_bp, url_prefix="/config")
    app.register_blueprint(user_bp, url_prefix="/user")
    # 定时器
    scheduler.init_app(app)
    import search.core.timer as search_timer
    search_timer.start()

    @app.before_request
    def before_request():
        logger.info(f"请求方式:{request.method}")
        logger.info(f"请求路径:{request.path}")
        logger.info(f"请求地址:{request.remote_addr}")
        logger.info(f"args:{simplejson.dumps(request.args)}")
        logger.info(f"form:{simplejson.dumps(request.form)}")
        logger.info(f"data:{request.data.decode()}")

    @app.after_request
    def after_request(response: Response):
        logger.info(f"响应路径:{request.path}")
        if len(simplejson.dumps(response.json)) < 1024:
            logger.info(f"响应json:{response.json}")
        logger.info(f"响应长度:{response.content_length}")
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
