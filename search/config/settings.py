# @Time    : 23/02/20 9:33
# @Author  : fyq
# @File    : settings.py
# @Software: PyCharm

__author__ = 'fyq'

import os

SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:123456@localhost:3306/search"
SQLALCHEMY_TRACK_MODIFICATIONS = False
SECRET_KEY = "search"
FILE_DIR = f"D:{os.path.sep}search_file"
SCHEDULER_TIMEZONE = 'Asia/Shanghai'
SCHEDULER_API_ENABLED = True
