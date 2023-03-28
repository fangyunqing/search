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


db: SQLAlchemy = SQLAlchemy()
migrate: Migrate = Migrate()
redis_pool = redis.ConnectionPool(host="127.0.0.1", port=6379)
thread_pool = ThreadPoolExecutor(max_workers=20)
scheduler = APScheduler()
