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
    # 排序号
    order = Column(Integer, default=0)
    # 是否可用
    usable = Column(String(1), default="1")
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())
    # 备注
    remark = Column(String(128))


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
    # 状态
    status = Column(String(128), default=constant.SearchStatus.PARSING)
    # 错误信息
    error = Column(String(1024))
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchCondition(db.Model, SerializerMixin):
    __tablename__ = "search_condition"
    serialize_only = ("id", "name", "display", "datatype", "order", "create_time")
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
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchField(db.Model, SerializerMixin):
    __tablename__ = "search_field"
    serialize_only = ("id", "name", "display", "datatype", "rule", "result_fields", "order",
                      "visible", "create_time")
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
    # 数据类型 int str date float
    datatype = Column(String(255), nullable=False, default="str")
    # 是否可见
    visible = Column(String(1), default='1')
    # 排序号
    order = Column(Integer, nullable=False)
    # 搜索的ID
    search_id = Column(Integer, ForeignKey("search.id"))
    # 生成路径
    search_field_gen_paths = db.relationship("SearchFieldGenPath", lazy='dynamic', backref="search_field")
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchSQLField(db.Model, SerializerMixin):
    __tablename__ = "search_sql_field"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 左侧
    left = Column(String(1024), nullable=False)
    # 右侧
    right = Column(String(1024))
    # 搜索SQL的ID
    search_sql_id = Column(Integer, ForeignKey("search_sql.id"))
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchSQL(db.Model, SerializerMixin):
    __tablename__ = "search_sql"
    serialize_only = ("id", "name", "display", "expression", "select_expression", "from_expression",
                      "where_expression", "other_expression", "order", "create_time")
    __table_args__ = (
        db.UniqueConstraint('id', 'name', name='uix_search_sql_id_name'),
    )
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 名称
    name = Column(String(255), nullable=False)
    # 展示名称
    display = Column(String(255), nullable=False)
    # expression
    expression = Column(Text, nullable=False)
    # select
    select_expression = Column(Text, nullable=True)
    # from
    from_expression = Column(Text, nullable=True)
    # where
    where_expression = Column(Text, nullable=True)
    # other
    other_expression = Column(Text, nullable=True)
    # 排序号
    order = Column(Integer, nullable=False)
    # 字段
    fields = db.relationship("SearchSQLField", lazy='dynamic', backref="search_sql")
    # 结果字段
    results = db.relationship("SearchSqlResult", lazy='dynamic', backref="search_sql")
    # 条件字段
    conditions = db.relationship("SearchSqlCondition", lazy='dynamic', backref="search_sql")
    # 搜索的ID
    search_id = Column(Integer, ForeignKey("search.id"))
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
    # 左边
    left = Column(String(1024))
    # 右边
    right = Column(String(1024))
    # 中间
    mid = Column(String(1024))
    # 生成时间
    create_time = Column(DateTime(timezone=True), default=func.now())


class SearchSqlCondition(db.Model, SerializerMixin):
    __tablename__ = "search_sql_condition"
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 搜索SQL的ID
    search_sql_id = Column(Integer, ForeignKey("search_sql.id"))
    # 左边
    left = Column(String(1024))
    # 右边
    right = Column(String(1024))
    # 中间
    mid = Column(String(1024))
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
