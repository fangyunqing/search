# @Time    : 2023/04/24 9:45
# @Author  : fyq
# @File    : bytes.py
# @Software: PyCharm

__author__ = 'fyq'


def format_bytes(size):
    if size < 1024:  # 比特
        size = str(round(size, 2)) + ' B'  # 字节
    elif 1024 <= size < 1024 * 1024:
        size = str(round(size / 1024, 2)) + ' KB'  # 千字节
    elif 1024 * 1024 <= size < 1024 * 1024 * 1024:
        size = str(round(size / 1024 / 1024, 2)) + ' MB'  # 兆字节
    elif 1024 * 1024 * 1024 <= size < 1024 * 1024 * 1024 * 1024:
        size = str(round(size / 1024 / 1024 / 1024, 2)) + ' GB'  # 千兆字节
    elif 1024 * 1024 * 1024 * 1024 <= size < 1024 * 1024 * 1024 * 1024 * 1024:
        size = str(round(size / 1024 / 1024 / 1024 / 1024, 2)) + ' TB'  # 太字节
    elif 1024 * 1024 * 1024 * 1024 * 1024 <= size < 1024 * 1024 * 1024 * 1024 * 1024 * 1024:
        size = str(round(size / 1024 / 1024 / 1024 / 1024 / 1024, 2)) + ' PB'  # 拍字节
    elif 1024 * 1024 * 1024 * 1024 * 1024 * 1024 <= size < 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024:
        size = str(round(size / 1024 / 1024 / 1024 / 1024 / 1024 / 1024, 2)) + ' EB'  # 艾字节
    return size
