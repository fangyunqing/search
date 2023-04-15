# gunicorn.conf
# coding:utf-8
# 并行工作进程数, int，cpu数量*2+1 推荐进程数
workers = 4
# 指定每个进程开启的线程数
threads = 1
# 绑定的ip与端口
bind = '0.0.0.0:8080'
# 设置守护进程,将进程交给supervisor管理
daemon = 'true'
# 工作模式协程，默认的是sync模式
worker_class = 'gevent'
# 设置最大并发量（每个worker处理请求的工作线程数，正整数，默认为1）
# worker_connections = 2000
# # 最大客户端并发数量，默认情况下这个值为1000。此设置将影响gevent和eventlet工作模式
# max_requests = 2000
# # 监听队列
# backlog = 512
# # 进程名
# proc_name = 'gunicorn_process'
# # 设置超时时间120s，默认为30s。
# timeout = 120
# # 超时重启
# graceful_timeout = 120
