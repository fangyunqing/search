# @Time    : 23/02/20 13:57
# @Author  : fyq
# @File    : __init__.py.py
# @Software: PyCharm

__author__ = 'fyq'

from concurrent.futures.thread import ThreadPoolExecutor

from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
import redis
from flask_apscheduler import APScheduler
from loguru import logger

from search.config.settings import REDIS_HOST, REDIS_PORT

db: SQLAlchemy = SQLAlchemy()
migrate: Migrate = Migrate()
redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT)
thread_pool = ThreadPoolExecutor(max_workers=20)
scheduler = APScheduler()

logger.add("search_{time:YYYY-MM-DD}.log", rotation="00:00",
           retention="60 days",
           compression="tar.gz",
           mode='a+',
           encoding='utf-8',
           backtrace=True,
           diagnose=True)
