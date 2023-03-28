# @Time    : 23/02/20 9:09
# @Author  : fyq
# @File    : manage.py
# @Software: PyCharm

__author__ = 'fyq'

from search import create_app

app = create_app()

if __name__ == '__main__':
    app.run()
