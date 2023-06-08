# @Time    : 23/02/20 9:33
# @Author  : fyq
# @File    : settings.py
# @Software: PyCharm

__author__ = 'fyq'

import os

SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:123456@10.109.88.52:3306/search"

SQLALCHEMY_TRACK_MODIFICATIONS = False

SQLALCHEMY_ECHO = False

SECRET_KEY = "search"

FILE_DIR = f"D:{os.path.sep}search_file"

SCHEDULER_TIMEZONE = 'Asia/Shanghai'

SCHEDULER_API_ENABLED = True

REDIS_HOST = "127.0.0.1"

REDIS_PORT = 6379
