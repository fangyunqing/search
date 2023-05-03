# @Time    : 2023/04/22 15:52
# @Author  : fyq
# @File    : db_search_pl_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import os
import shutil
import uuid
from abc import ABCMeta, abstractmethod
from threading import get_ident
from typing import Optional, List, Any

import munch
import polars as pl
from loguru import logger
from pyext import RuntimeModule

from search import dm
from search.core.decorator import search_cost_time
from search.core.progress import Progress
from search.core.search_context import SearchContext
from search.core.strategy import FetchLengthStrategy
from search.exceptions import SearchException

pl.Config(activate_decimals=True, set_fmt_str_lengths=100)


class DBSearchPolarsCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pl.DataFrame]:
        pass


class AbstractDBSearchPolarsCache(DBSearchPolarsCache):

    @search_cost_time
    def get_data(self, search_context: SearchContext, top: bool = False) -> Optional[pl.DataFrame]:
        # 所有的连接数
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
                tmp_sql_list: List[str] = []
                select_expression = search_buffer.search_sql.select_expression
                if select_expression:
                    select_zero_expression = f"{select_expression} TOP 0"
                    if search_cache_index == 0 and top:
                        select_expression = f"{select_expression} TOP {search_context.search.top}"
                else:
                    select_zero_expression = "TOP 0"
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
                    tmp_tables.append(f"{tmp_tablename}_{tf}")
                    ts = ["SELECT",
                          select_zero_expression,
                          tf_l + f" INTO {tmp_tablename}_{tf}",
                          "FROM", search_buffer.search_sql.from_expression]
                    if len(join_expression) > 0:
                        ts.append(" ".join(join_expression))
                    if len(where_expression) > 0:
                        ts.append("WHERE")
                        ts.append(where_expression)
                    ts.append(search_buffer.search_sql.other_expression)
                    tmp_sql_list.append(" ".join(ts))
                    # 添加索引
                    tmp_sql_list.append(f"CREATE CLUSTERED INDEX idx_{tf} ON {tmp_tablename}_{tf}({tf_l})")
                    if select_expression:
                        ts[1] = select_expression if "TOP" in select_expression \
                            else select_expression + " TOP 100 PERCENT"
                    else:
                        ts[1] = "TOP 100 PERCENT"
                    ts[2] = tf_l
                    ts.insert(0, f"SELECT DISTINCT {tf_l} FROM (")
                    ts.insert(0, f"INSERT INTO {tmp_tablename}_{tf}")
                    ts.append(") A")
                    tmp_sql_list.append(" ".join(ts))

                sql = " ".join(sql_list)

                file_info_list.append(self.exec(conn_list=
                                                [conn_list[0]] if search_cache_index == 0 and top else conn_list,
                                                search_context=search_context,
                                                search_buffer=search_buffer,
                                                sql=sql,
                                                tmp_dir=tmp_dir,
                                                tmp_sql_list=tmp_sql_list))

            for file_info_index, file_info in enumerate(file_info_list):
                sql_dir = file_info.get("dir")

                if file_info_index == 0:
                    data_df = pl.scan_parquet(f"{sql_dir}{os.sep}*.parquet")
                else:
                    new_df = pl.scan_parquet(f"{sql_dir}{os.sep}*.parquet")
                    data_df = data_df.join(other=new_df,
                                           how=file_info.get('search_buffer').search_sql.how,
                                           on=file_info.get('search_buffer').join_fields)

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
             tmp_sql_list: List[str]) -> Any:
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
        for field in search_context.search_md5.search_original_field_list:
            new_field = "col_" + field
            if field in search_field_dict:
                search_field = search_field_dict[field]
                try:
                    if search_field.rule.startswith(("def", "import", "from")):
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
                        if field in df.columns:
                            expr_list.append(pl.col(field).alias(new_field))
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
                    expr_list[-1] = expr.cast(pl.Decimal(precision=20, scale=8))
                elif search_field.datatype == "date":
                    expr_list[-1] = expr.cast(pl.Date)
                elif search_field.datatype == "datetime":
                    expr_list[-1] = expr.cast(pl.Datetime)
            else:
                expr_list.append(pl.lit(None).alias(new_field))

        df = df.with_columns(*expr_list) \
            .select(["col_" + field for field in search_context.search_md5.search_original_field_list]) \
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
             tmp_sql_list: List[str]) -> Any:
        sql_tmp_dir = f"{tmp_dir}{os.sep}{search_buffer.search_sql.name}"
        os.makedirs(sql_tmp_dir)
        expr_list = []
        for select_field in search_buffer.select_fields:
            if select_field.startswith("i"):
                expr_list.append(pl.col(select_field).cast(pl.Int32))
            elif select_field.startswith("n"):
                expr_list.append(pl.col(select_field).cast(pl.Decimal(precision=20, scale=8)))
            elif select_field.startswith("u"):
                expr_list.append(pl.col(select_field).apply(lambda x: str(x)).cast(pl.Utf8))
            elif select_field.startswith("s"):
                expr_list.append(pl.col(select_field).cast(pl.Utf8))
            elif select_field.startswith("t"):
                expr_list.append(pl.col(select_field).cast(pl.Datetime))
            elif select_field.startswith("d"):
                expr_list.append(pl.col(select_field).cast(pl.Date))
            elif select_field.startswith("b"):
                expr_list.append(pl.col(select_field).cast(pl.Boolean))
            else:
                raise SearchException(f"sql查询[{search_buffer.name}]中查询字段或者关联字段[{select_field}"
                                      f"请以(i,n,u,s,t,d,b)开头")
        fetch_len = self.fetch_strategy.get_fetch_rows(search_buffer.select_fields)
        for conn in conn_list:
            cur = conn.cursor()
            try:
                for tmp_sql in tmp_sql_list:
                    logger.info(f"{tmp_sql} {search_buffer.args}")
                    cur.execute(tmp_sql, tuple(search_buffer.args))
                logger.info(f"{sql} {search_buffer.args}")
                cur.execute(sql, tuple(search_buffer.args))
                index = 0
                while True:
                    datas = cur.fetchmany(fetch_len)
                    if not datas:
                        break
                    pl.DataFrame(datas, infer_schema_length=None).with_columns(*expr_list)\
                        .write_parquet(f"{sql_tmp_dir}{os.sep}{index}.parquet")
                    index += 1
            except Exception as e:
                if hasattr(e, "args") and e.args[0] != 208:
                    raise e
                else:
                    logger.warning(e)
            finally:
                cur.close()
        return {
            "search_buffer": search_buffer,
            "dir": sql_tmp_dir
        }


@Progress(prefix="export", suffix="db")
class DefaultDBExportPolarsCache(DefaultDBPolarsCache):
    pass


@Progress(prefix="search", suffix="db")
class DefaultDBSearchPolarsCache(DefaultDBPolarsCache):
    pass
