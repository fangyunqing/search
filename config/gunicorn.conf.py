# gunicorn.conf
# coding:utf-8
import multiprocessing
# 并行工作进程数, int，cpu数量*2+1 推荐进程数
workers = multiprocessing.cpu_count() * 2 + 1
# 工作方式
worker_class = "gthread"
# 指定每个进程开启的线程数
threads = 100
# 绑定的ip与端口
bind = '0.0.0.0:8080'
# 最大客户端并发数量，默认情况下这个值为1000
max_requests = 1000
# 监听队列
backlog = 128
# 进程名
proc_name = 'gunicorn_process'
# 设置超时时间120s，默认为30s。
timeout = 360
# 超时重启
graceful_timeout = 360
