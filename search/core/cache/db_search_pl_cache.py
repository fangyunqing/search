# @Time    : 2023/04/22 15:52
# @Author  : fyq
# @File    : db_search_pl_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import glob
import os
import shutil
import uuid
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from threading import get_ident
from typing import Optional, List, Any

import munch
import polars as pl
import pymssql
import pyodbc
from loguru import logger
from pyext import RuntimeModule

from search import dm
from search.core.decorator import search_cost_time
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.core.strategy import FetchLengthStrategy, search_strategy
from search.exceptions import FieldNameException
from search.util.field import get_sql_field_type, get_pl_expr

pl.Config(activate_decimals=True, set_fmt_str_lengths=100)


@dataclass
class TempTableStatement:
    # 创建临时表语句
    create_stat: str = ""
    # 创建索引
    index_stat: str = ""
    # 插入语句
    insert_stat: str = ""
    # 插入语句2
    execute_insert_stat: str = ""
    # 字段
    fields: List[str] = field(default_factory=lambda: [])


class DBSearchPolarsCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pl.DataFrame]:
        pass


class AbstractDBSearchPolarsCache(DBSearchPolarsCache):

    @search_strategy.add_lock
    @search_cost_time
    def get_data(self, search_context: SearchContext, top: bool = False) -> Optional[pl.DataFrame]:
        # 所有的连接数
        if top:
            conn_list = [dm.get_main_connection()]
        else:
            conn_list = dm.get_connections()
        # 执行步骤
        self.count(search_context=search_context)
        # 总数据
        data_df: Optional[pl.LazyFrame] = None
        # 临时文件夹
        tmp_dir = f"{search_context.cache_dir}{os.sep}tmp{os.sep}{uuid.uuid4()}"
        os.makedirs(tmp_dir, exist_ok=True)
        # 创建的临时表
        tmp_tables = []
        # 文件信息
        file_info_list = []
        try:
            for search_cache_index, search_buffer in enumerate(search_context.search_buffer_list):
                tmp_tablename = search_buffer.tmp_tablename.format(get_ident())
                sql_list: List[str] = []
                tmp_sql_list: List[TempTableStatement] = []
                select_expression = search_buffer.search_sql.select_expression
                if select_expression:
                    if search_cache_index == 0 and top:
                        select_expression = f"{select_expression} TOP {search_context.search.top}"
                else:
                    if search_cache_index == 0 and top:
                        select_expression = f"TOP {search_context.search.top}"

                where_expression = search_buffer.where_expression
                join_expression = [s.format(*[get_ident() for _ in range(0, s.count("#{}"))])
                                   for s in search_buffer.join_expression]

                sql_list.append("SELECT")
                if select_expression:
                    sql_list.append(select_expression)
                sql_list.append(",".join(search_buffer.field_list))
                sql_list.append("FROM")
                sql_list.append(search_buffer.search_sql.from_expression)
                if len(join_expression) > 0:
                    sql_list.append(" ".join(join_expression))
                if len(where_expression) > 0:
                    sql_list.append("WHERE")
                    sql_list.append(where_expression)
                sql_list.append(search_buffer.search_sql.other_expression)

                for tmp_fields in search_buffer.tmp_fields_list:
                    tf = "_".join(tmp_fields)
                    tf_l = ",".join(tmp_fields)
                    create_tb_name = f"{tmp_tablename}_{tf}"
                    tmp_tables.append(create_tb_name)
                    tts = TempTableStatement()
                    tts.create_stat = f"CREATE TABLE {create_tb_name}" \
                                      f"({','.join([get_sql_field_type(_) for _ in tmp_fields])})"
                    tts.index_stat = f"CREATE CLUSTERED INDEX idx_{tf} ON {create_tb_name}({tf_l})"

                    if select_expression:
                        select_top = select_expression if "TOP" in select_expression \
                            else select_expression + " TOP 100 PERCENT"
                    else:
                        select_top = "TOP 100 PERCENT"
                    ts = [f"INSERT INTO {create_tb_name}",
                          f"SELECT DISTINCT {tf_l} FROM ("
                          f"SELECT {select_top}",
                          tf_l,
                          "FROM", search_buffer.search_sql.from_expression]
                    if len(join_expression) > 0:
                        ts.append(" ".join(join_expression))
                    if len(where_expression) > 0:
                        ts.append("WHERE")
                        ts.append(where_expression)
                    ts.append(search_buffer.search_sql.other_expression)
                    ts.append(") A")
                    tts.insert_stat = " ".join(ts)
                    tts.fields = tmp_fields
                    tts.execute_insert_stat = f"INSERT INTO {create_tb_name} ({','.join(tmp_fields)}) " \
                                              f"VALUES ({','.join([f'?' for _ in tmp_fields])})"
                    tmp_sql_list.append(tts)

                sql = " ".join(sql_list)

                if top:
                    new_df = self.exec_with_top(search_context=search_context,
                                                search_buffer=search_buffer,
                                                sql=sql,
                                                conn_list=conn_list,
                                                tmp_sql_list=tmp_sql_list)
                    if new_df is not None:
                        if data_df is None:
                            data_df = new_df
                        else:
                            data_df = data_df.join(other=new_df,
                                                   how=search_buffer.search_sql.how,
                                                   on=search_buffer.join_fields)
                else:
                    file_info_list.append(self.exec(conn_list=conn_list,
                                                    search_context=search_context,
                                                    search_buffer=search_buffer,
                                                    sql=sql,
                                                    tmp_dir=tmp_dir,
                                                    tmp_sql_list=tmp_sql_list))

            if len(file_info_list) > 0:
                for file_info_index, file_info in enumerate(file_info_list):
                    sql_dir = f"{file_info.get('dir')}{os.sep}*.parquet"
                    if len(glob.glob(sql_dir)) > 0:
                        if file_info_index == 0:
                            data_df = pl.scan_parquet(sql_dir)
                        else:
                            new_df = pl.scan_parquet(sql_dir)
                            data_df = data_df.join(other=new_df,
                                                   how=file_info.get('search_buffer').search_sql.how,
                                                   on=file_info.get('search_buffer').join_fields)

            if data_df is None:
                return pl.DataFrame()
            return self.exec_new_df(search_context=search_context,
                                    df=data_df)
        finally:
            if len(tmp_tables) > 0:
                for conn in conn_list:
                    cur = conn.cursor()
                    try:
                        for ttb in tmp_tables:
                            try:
                                cur.execute(f"DROP TABLE {ttb}")
                            except Exception as e:
                                logger.warning(str(e))
                    finally:
                        cur.close()
            [conn.close() for conn in conn_list]
            shutil.rmtree(tmp_dir)

    @abstractmethod
    def count(self, search_context: SearchContext):
        pass

    @abstractmethod
    def exec(self,
             search_context: SearchContext,
             search_buffer: munch.Munch,
             tmp_dir: str,
             conn_list,
             sql: str,
             tmp_sql_list: List[TempTableStatement]) -> Any:
        pass

    @abstractmethod
    def exec_with_top(self,
                      search_context: SearchContext,
                      search_buffer: munch.Munch,
                      conn_list,
                      sql: str,
                      tmp_sql_list: List[TempTableStatement]) -> Optional[pl.LazyFrame]:
        pass

    @abstractmethod
    def exec_new_df(self, search_context: SearchContext, df: pl.LazyFrame) -> pl.DataFrame:
        pass


class DefaultDBPolarsCache(AbstractDBSearchPolarsCache):

    def __init__(self):
        self.fetch_strategy = FetchLengthStrategy()

    def exec_new_df(self, search_context: SearchContext, df: pl.LazyFrame) -> pl.DataFrame:
        search_field_dict = {search_field.name: search_field
                             for search_field in search_context.search_field_list}
        expr_list = []
        for field_name in search_context.search_md5.search_original_field_list:
            new_field = "col_" + field_name
            if field_name in search_field_dict:
                search_field = search_field_dict[field_name]
                try:
                    if search_field.rule and search_field.rule.startswith(("def", "import", "from")):
                        md = RuntimeModule.from_string('a', search_field.rule)
                        find = False
                        for v in md.__dict__.values():
                            if callable(v):
                                expr_list.append(v().alias(new_field))
                                find = True
                                break
                        if not find:
                            expr_list.append(pl.lit(None).alias(new_field))
                            logger.warning(f"{search_context.search_md5.search_name}-{search_field.name}-rule未发现可执行函数")
                    else:
                        if field_name in df.columns:
                            expr_list.append(pl.col(field_name).alias(new_field))
                        else:
                            expr_list.append(pl.lit(None).alias(new_field))
                            logger.warning(f"查询结果中未包含列[{search_field.name}]")
                except Exception as e:
                    expr_list.append(pl.lit(None).alias(new_field))
                    logger.exception(e)
                expr = expr_list[-1]
                if search_field.datatype == "str":
                    expr_list[-1] = expr.cast(pl.Utf8)
                elif search_field.datatype == "int":
                    expr_list[-1] = expr.cast(pl.Int32)
                elif search_field.datatype == "float":
                    expr_list[-1] = expr.cast(pl.Float32)
                elif search_field.datatype == "date":
                    expr_list[-1] = expr.cast(pl.Date)
                elif search_field.datatype == "datetime":
                    expr_list[-1] = expr.cast(pl.Datetime)
            else:
                expr_list.append(pl.lit(None).alias(new_field))

        df = df.with_columns(*expr_list) \
            .select(["col_" + field_name for field_name in search_context.search_md5.search_original_field_list]) \
            .collect()
        df.columns = search_context.search_md5.search_original_field_list
        return df

    execs = ["exec", "exec_new_df"]

    def count(self, search_context: SearchContext):
        return len(search_context.search_buffer_list) + 1

    def exec(self,
             search_context: SearchContext,
             search_buffer: munch.Munch,
             tmp_dir: str,
             conn_list,
             sql: str,
             tmp_sql_list: List[TempTableStatement]) -> Any:
        # 创建目录
        sql_tmp_dir = f"{tmp_dir}{os.sep}{search_buffer.search_sql.name}"
        os.makedirs(sql_tmp_dir)
        # 创建pl表达式
        expr_list = []
        for select_field in search_buffer.select_fields:
            expr = get_pl_expr(select_field)
            if expr is not None:
                expr_list.append(expr)
            else:
                raise FieldNameException(search_buffer.name, select_field)

        fetch_len = self.fetch_strategy.get_fetch_rows(search_buffer.select_fields)
        for conn in conn_list:
            cur = conn.cursor()
            try:
                cur._cursor.fast_executemany = True
                # 执行查询
                logger.info(f"{sql} {search_buffer.args}")
                try:
                    cur.execute(sql, tuple(search_buffer.args))
                    index = 0
                    while True:
                        datas = cur.fetchmany(fetch_len)
                        if not datas:
                            break
                        logger.info(f"fetchmany {len(datas)}")
                        pl.DataFrame([list(d) for d in datas],
                                     schema=search_buffer.select_fields,
                                     infer_schema_length=None).with_columns(*expr_list) \
                            .write_parquet(f"{sql_tmp_dir}{os.sep}{index}.parquet", use_pyarrow=True)
                        index += 1
                except pyodbc.ProgrammingError as e:
                    if e.args[0] != "42S02":
                        raise e

                for tts in tmp_sql_list:
                    # 创建表
                    logger.info(f"{tts.create_stat}")
                    cur.execute(tts.create_stat)
                    # 创建索引
                    logger.info(f"{tts.index_stat}")
                    cur.execute(tts.index_stat)
                    # 插入语句
                    sql_dir = f"{sql_tmp_dir}{os.sep}*.parquet"
                    if len(glob.glob(sql_dir)) > 0:
                        logger.info(tts.execute_insert_stat)
                        datas = pl.scan_parquet(sql_dir).select(tts.fields).unique().collect().to_numpy().tolist()
                        if datas:
                            cur.executemany(tts.execute_insert_stat, datas)
            finally:
                cur.close()

        return {
            "search_buffer": search_buffer,
            "dir": sql_tmp_dir
        }

    def exec_with_top(self,
                      search_context: SearchContext,
                      search_buffer: munch.Munch,
                      conn_list,
                      sql: str,
                      tmp_sql_list: List[TempTableStatement]) -> Optional[pl.LazyFrame]:
        expr_list = []
        for select_field in search_buffer.select_fields:
            expr = get_pl_expr(select_field)
            if expr is not None:
                expr_list.append(expr)
            else:
                raise FieldNameException(search_buffer.name, select_field)

        conn = conn_list[0]
        cur = conn.cursor()
        try:
            for tts in tmp_sql_list:
                # 创建表
                logger.info(f"{tts.create_stat}")
                cur.execute(tts.create_stat)
                # 创建索引
                logger.info(f"{tts.index_stat}")
                cur.execute(tts.index_stat)
                # 插入语句
                logger.info(f"{tts.insert_stat} {tuple(search_buffer.args)}")
                cur.execute(tts.insert_stat, tuple(search_buffer.args))

            logger.info(f"{sql} {tuple(search_buffer.args)}")
            cur.execute(sql, tuple(search_buffer.args))
            datas = cur.fetchall()
            if datas:
                return pl.DataFrame([list(d) for d in datas],
                                    schema=search_buffer.select_fields,
                                    infer_schema_length=None).lazy().with_columns(*expr_list)
        except pyodbc.ProgrammingError as e:
            if e.args[0] != "42S02":
                raise e
        finally:
            cur.close()


@Progress(prefix="export", suffix="db")
class DefaultDBExportPolarsCache(DefaultDBPolarsCache):
    pass


@Progress(prefix="search", suffix="db")
class DefaultDBSearchPolarsCache(DefaultDBPolarsCache):
    pass
