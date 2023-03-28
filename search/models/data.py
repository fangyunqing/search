# @Time    : 23/02/20 9:47
# @Author  : fyq
# @File    : data.py
# @Software: PyCharm

__author__ = 'fyq'

from sqlalchemy import Column, Integer, String, ForeignKey, Text, DateTime, func, BigInteger
from sqlalchemy_serializer import SerializerMixin

import search.constant as constant
from search.extend import db


class SearchDatasource(db.Model, SerializerMixin):
    __tablename__ = "search_datasource"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 名称
    name = Column(String(255), nullable=False, unique=True)
    # 是否主数据库
    major = Column(String(1), default="1")
    # ip
    ip = Column(String(128), nullable=False)
    # port
    port = Column(Integer, nullable=False)
    # 数据库名
    db_name = Column(String(128), nullable=False)
    # db_type
    db_type = Column(String(128), nullable=False)
    # 用户名
    user_name = Column(String(128), nullable=False)
    # 密码
    password = Column(String(128), nullable=False)
    # 备注
    remark = Column(String(128))
    # 是否可用
    usable = Column(String(1), default="1")
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())
    # 排序号
    order = Column(Integer, default=0)


class Search(db.Model, SerializerMixin):
    __tablename__ = "search"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 名称
    name = Column(String(255), nullable=False, unique=True)
    # 展示名称
    display = Column(String(255), nullable=False)
    # 是否可用
    usable = Column(Integer, default=1)
    # 页大小
    page_size = Column(Integer, default=50)
    # 查询的时候 先查询多少条
    top = Column(Integer, default=2000)
    # redis缓存时间 单位秒
    redis_cache_time = Column(Integer, default=300)
    # 多少条形成csv
    want_csv = Column(Integer, default=10000)
    # 单个导出的文件大小
    export_single_size = Column(Integer, default=200000)
    # 导出文件格式
    export_file_type = Column(String(128), default="xlsx")
    # 导出文件缓存时间 单位分钟
    export_file_cache_time = Column(Integer, default=30)
    # 前后页数
    pages = Column(Integer, default=10)
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())
    # 状态
    status = Column(String(128), default=constant.SearchStatus.PARSING)


class SearchCondition(db.Model, SerializerMixin):
    __tablename__ = "search_condition"
    serialize_only = ("id", "name", "display", "datatype", "order", "usable", "create_time")
    __table_args__ = (
        db.UniqueConstraint('id', 'name', name='uix_search_condition_id_name'),
    )
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 名称
    name = Column(String(255), nullable=False)
    # 展示名称
    display = Column(String(255), nullable=False)
    # 数据类型 int str date list float
    datatype = Column(String(255), nullable=False, default="str")
    # 排序号
    order = Column(Integer, nullable=False)
    # 搜索的ID
    search_id = Column(Integer, ForeignKey("search.id"))
    # 是否可用
    usable = Column(Integer, default=1)
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchField(db.Model, SerializerMixin):
    __tablename__ = "search_field"
    serialize_only = ("id", "name", "display", "datatype", "rule", "result_fields", "order",
                      "visible", "usable", "create_time")
    __table_args__ = (
        db.UniqueConstraint('id', 'name', name='uix_search_field_id_name'),
    )
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 名称
    name = Column(String(255), nullable=False)
    # 展示名称
    display = Column(String(255), nullable=False)
    # 规则
    rule = Column(String(1024), nullable=False)
    # 结果字段
    result_fields = Column(String(2048))
    # 规则是否脚本
    script = Column(String(1), default='0')
    # 数据类型 int str date list float
    datatype = Column(String(255), nullable=False, default="str")
    # 是否可见
    visible = Column(String(1), default='1')
    # 排序号
    order = Column(Integer, nullable=False)
    # 搜索的ID
    search_id = Column(Integer, ForeignKey("search.id"))
    # 是否可用
    usable = Column(Integer, default=1)
    # 生成路径
    search_field_gen_paths = db.relationship("SearchFieldGenPath", lazy='dynamic', backref="search_field")
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchSQLGenField(db.Model, SerializerMixin):
    __tablename__ = "search_sql_gen_field"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 生成的字段
    gen_field = Column(String(1024), nullable=False)
    # 字段表达式
    exp_field = Column(String(1024))
    # 搜索SQL的ID
    search_sql_id = Column(Integer, ForeignKey("search_sql.id"))
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchSQL(db.Model, SerializerMixin):
    __tablename__ = "search_sql"
    serialize_only = ("id", "name", "display", "exp", "order", "usable", "create_time")
    __table_args__ = (
        db.UniqueConstraint('id', 'name', name='uix_search_sql_id_name'),
    )
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 名称
    name = Column(String(255), nullable=False)
    # 展示名称
    display = Column(String(255), nullable=False)
    # sql
    exp = Column(Text, nullable=False)
    # 排序号
    order = Column(Integer, nullable=False)
    # 结果字段
    result_fields = db.relationship("SearchSqlResult", lazy='dynamic', backref="search_sql")
    # 条件字段
    condition_fields = Column(String(2048))
    # 搜索的ID
    search_id = Column(Integer, ForeignKey("search.id"))
    # select 语句
    select_exp = Column(String(256))
    # from后面的语句
    from_exp = Column(Text, nullable=True)
    # 是否可用
    usable = Column(Integer, default=1)
    # 字段
    search_sql_gen_fields = db.relationship("SearchSQLGenField", lazy='dynamic', backref="search_sql")
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchFieldGenPath(db.Model, SerializerMixin):
    __tablename__ = "search_field_gen_path"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 搜索字段的ID
    search_field_id = Column(Integer, ForeignKey("search_field.id"))
    # 搜索SQL的ID
    search_sql_id = Column(Integer, ForeignKey("search_sql.id"))
    # 依赖的字段
    depend_field = Column(String(1024), nullable=False)
    # 排序号
    order = Column(Integer, nullable=False)
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchSqlResult(db.Model, SerializerMixin):
    __tablename__ = "search_sql_result"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 搜索SQL的ID
    search_sql_id = Column(Integer, ForeignKey("search_sql.id"))
    # 结果字段
    result_field = Column(String(1024))
    # 字段
    field_name = Column(String(1024))
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchFile(db.Model, SerializerMixin):
    __tablename__ = "search_file"
    serialize_only = ("id", "file_name", "size")
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 搜索的ID
    search_id = Column(Integer, ForeignKey("search.id"))
    # mad5
    search_md5 = Column(String(128), nullable=False)
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())
    # 路径
    path = Column(String(128), nullable=False)
    # 用途
    use = Column(String(128), default="search")
    # 文件大小
    size = Column(BigInteger, nullable=False)
    # 文件状态
    status = Column(String(128), default=constant.FileStatus.USABLE)
    # 文件名
    file_name = Column(String(128), nullable=True)
