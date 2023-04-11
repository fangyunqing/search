# @Time    : 2023/04/11 9:48
# @Author  : fyq
# @File    : gunicorn.conf.py.py
# @Software: PyCharm

__author__ = 'fyq'

workers = 5
worker_class = "gevent"
bind = "0.0.0.0:8080"
