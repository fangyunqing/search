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


# gunicorn启动前执行
def on_starting(server):
    import redis
    from search.config.settings import REDIS_HOST, REDIS_PORT, SQLALCHEMY_DATABASE_URI
    from search import constant
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from search.models import SearchParameter
    from typing import List
    import simplejson

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    r.delete(constant.RedisKey.SEARCH_CONTEXT_LOCK)
    r.delete(constant.RedisKey.SEARCH_DELETE_FILE_LOCK)
    r.delete(constant.RedisKey.SEARCH_STRATEGY)
    r.delete(constant.RedisKey.SEARCH_STRATEGY_LOCK)
    r.delete(constant.RedisKey.SEARCH_CONFIG)
    keys = r.keys(f"{constant.RedisKey.THREAD_LOCK_PREFIX}_*")
    if len(keys) > 0:
        r.delete(*keys)
    keys = r.keys(f"{constant.RedisKey.PROGRESS_LOCK_PREFIX}_*")
    if len(keys) > 0:
        r.delete(*keys)

    db = create_engine(SQLALCHEMY_DATABASE_URI)
    obj_session = sessionmaker(db)
    db_session = obj_session()
    params: List[SearchParameter] = db_session.query(SearchParameter).all()
    r.set(name=constant.RedisKey.SEARCH_CONFIG,
          value=simplejson.loads([p.to_dict() for p in params]))
