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
from loguru import logger

from search import dm

import pandas as pd
import polars as pl

from search.core.progress import Progress
from search.core.search_context import SearchContext, SearchBuffer

from pyext import RuntimeModule


class DBSearchPolarsCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        pass


class AbstractDBSearchPolarsCache(DBSearchPolarsCache):

    def get_data(self, search_context: SearchContext, top: bool = False) -> Optional[pd.DataFrame]:
        conn_list = dm.get_connections()
        self.count(search_context=search_context, conn_list=conn_list, top=top)
        data_df: Optional[pl.LazyFrame] = None
        tmp_file = f"{search_context.cache_dir}{os.sep}tmp{os.sep}{uuid.uuid4()}"
        try:
            os.makedirs(tmp_file, exist_ok=True)
            file_info_list = []
            for search_cache_index, search_buffer in enumerate(search_context.search_buffer_list):
                tmp_tablename = search_buffer.tmp_tablename.format(get_ident())
                sql_list: List[str] = []
                tmp_sql_list: List[str] = []
                select_expression = search_buffer.search_sql.select_expression
                if search_cache_index == 0:
                    if top:
                        select_expression = f"{select_expression} top {search_context.search.top}"

                where_expression = search_buffer.where_expression
                where_expression = where_expression. \
                    format(*[get_ident() for _ in range(0, search_buffer.where_expression.count("{}"))])
                sql_list.append("select")
                sql_list.append(select_expression)
                sql_list.append(",".join(search_buffer.field_list))
                sql_list.append("from")
                sql_list.append(search_buffer.search_sql.from_expression)
                if len(where_expression) > 0:
                    sql_list.append("where")
                    sql_list.append(where_expression)
                sql_list.append(search_buffer.search_sql.other_expression)

                tmp_sql_list.append("select")
                tmp_sql_list.append(select_expression)
                tmp_sql_list.append(",".join(search_buffer.tmp_fields) + f" into {tmp_tablename}")
                tmp_sql_list.append("from")
                tmp_sql_list.append(search_buffer.search_sql.from_expression)
                if len(where_expression) > 0:
                    tmp_sql_list.append("where")
                    tmp_sql_list.append(where_expression)
                tmp_sql_list.append(search_buffer.search_sql.other_expression)

                sql = " ".join(sql_list)
                tmp_sql = " ".join(tmp_sql_list)

                # 类型判断
                data_type = {}
                expr_list = []
                for select_field in search_buffer.select_fields:
                    if select_field.startswith("i"):
                        data_type[select_field] = pl.Int32
                    elif select_field.startswith("n"):
                        data_type[select_field] = pl.Float32
                    elif select_field.startswith("u"):
                        data_type[select_field] = pl.Object
                        expr_list.append(pl.col(select_field).apply(lambda x: str(x)).cast(pl.Utf8))
                    elif select_field.startswith("s"):
                        data_type[select_field] = pl.Utf8
                    elif select_field.startswith("t"):
                        data_type[select_field] = pl.Datetime
                    elif select_field.startswith("d"):
                        data_type[select_field] = pl.Date
                    elif select_field.startswith("b"):
                        data_type[select_field] = pl.Boolean
                # 创建多级文件夹
                index = 0
                file_info_list.append({
                    "file": f"{tmp_file}{os.sep}{search_buffer.search_sql.name}",
                    "search_buffer": search_buffer
                })
                for conn in conn_list:
                    res = self.exec(conn=conn,
                                    search_context=search_context,
                                    search_buffer=search_buffer,
                                    sql=sql,
                                    tmp_sql=tmp_sql)
                    for r in res:
                        file = f"{tmp_file}{os.sep}{search_buffer.search_sql.name}-{index}.parquet"
                        pl.DataFrame(data=r, schema=data_type).with_columns(*expr_list).write_parquet(file)
                        index += 1
                    if search_cache_index == 0 and top:
                        break
                #
                # if len(data) > 0:
                #     data_type = {}
                #     expr_list = []
                #     for select_field in search_buffer.select_fields:
                #         if select_field.startswith("i"):
                #             data_type[select_field] = pl.Int32
                #         elif select_field.startswith("n"):
                #             data_type[select_field] = pl.Float32
                #         elif select_field.startswith("u"):
                #             data_type[select_field] = pl.Object
                #             expr_list.append(pl.col(select_field).apply(lambda x: str(x)).cast(pl.Utf8))
                #         elif select_field.startswith("s"):
                #             data_type[select_field] = pl.Utf8
                #         elif select_field.startswith("t"):
                #             data_type[select_field] = pl.Datetime
                #         elif select_field.startswith("d"):
                #             data_type[select_field] = pl.Date
                #         elif select_field.startswith("b"):
                #             data_type[select_field] = pl.Boolean
                #     # 构建Lazy
                #     new_df = pl.LazyFrame(data=data, schema=data_type).with_columns(*expr_list)
                #     # 释放内存
                #     del data
                #     if data_df is None:
                #         data_df = new_df
                #     else:
                #         data_df = data_df.join(other=new_df,
                #                                how=search_buffer.search_sql.how,
                #                                left_on=search_buffer.join_fields,
                #                                right_on=search_buffer.join_fields)

            for file_info_index, file_info in enumerate(file_info_list):
                if file_info_index == 0:
                    data_df = pl.scan_parquet(f"{file_info.get('file')}*.parquet")
                else:
                    new_df = pl.scan_parquet(f"{file_info.get('file')}*.parquet")
                    data_df = data_df.join(other=new_df,
                                           how=file_info.get('search_buffer').search_sql.how,
                                           on=file_info.get('search_buffer').join_fields)

            return self.exec_new_df(search_context=search_context,
                                    df=data_df)
        finally:
            [conn.close() for conn in conn_list]
            # shutil.rmtree(tmp_file)

    @abstractmethod
    def count(self, conn_list: List, search_context: SearchContext, top: bool):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, search_buffer: munch.Munch, conn, sql: str, tmp_sql: str) -> Any:
        pass

    @abstractmethod
    def exec_new_df(self, search_context: SearchContext, df: pl.LazyFrame) -> pd.DataFrame:
        pass


class DefaultDBPolarsCache(AbstractDBSearchPolarsCache):

    def exec_new_df(self, search_context: SearchContext, df: pl.LazyFrame) -> pd.DataFrame:
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
                                expr_list.append(pl.lit(None).alias(new_field))
                                find = True
                                break
                        if not find:
                            expr_list.append(pl.lit(None).alias(new_field))
                            logger.warning(f"{search_context.search_md5.search_name}-{search_field.name}-rule未发现可执行函数")
                    else:
                        if field in df:
                            expr_list.append(pl.col(field).alias(new_field))
                        else:
                            expr_list.append(pl.lit(None).alias(new_field))
                            logger.warning(f"查询结果中未包含列[{search_field.name}]")
                except Exception as e:
                    expr_list.append(pl.lit(None).alias(new_field))
                    logger.exception(e)
                expr = expr_list[-1]
                if search_field.datatype == "str":
                    expr.cast(pl.Utf8)
                elif search_field.datatype == "int":
                    expr.cast(pl.Int32)
                elif search_field.datatype == "float":
                    expr.cast(pl.Decimal)
                elif search_field.datatype == "date":
                    expr.cast(pl.Datetime)
            else:
                expr_list.append(pl.lit(None).alias(new_field))

        df = df.with_columns(*expr_list) \
            .select(["col_" + field for field in search_context.search_md5.search_original_field_list]) \
            .collect()
        df.columns = search_context.search_md5.search_original_field_list
        return df.to_pandas(use_pyarrow_extension_array=True)

    execs = ["exec", "exec_new_df"]

    def count(self, conn_list: List, search_context: SearchContext, top: bool):
        c = len(conn_list) * len(search_context.search_buffer_list)
        if top:
            return c - len(conn_list) + 2
        else:
            return c + 1

    def exec(self, search_context: SearchContext, search_buffer: SearchBuffer, conn, sql: str, tmp_sql: str) -> Any:
        cur = conn.cursor()
        try:
            if len(search_buffer.tmp_fields) > 0:
                logger.info(f"临时表sql:{tmp_sql} 参数:{search_buffer.args}")
                cur.execute(tmp_sql, tuple(search_buffer.args))
            logger.info(f"查询表sql:{sql} 参数:{search_buffer.args}")
            cur.execute(sql, tuple(search_buffer.args))
            while True:
                data = cur.fetchmany(100000)
                if data:
                    yield data
                else:
                    break
        except Exception as e:
            if hasattr(e, "args") and e.args[0] != 208:
                raise e
            else:
                logger.warning(e)
        finally:
            cur.close()


@Progress(prefix="export", suffix="db")
class DefaultDBExportPolarsCache(DefaultDBPolarsCache):
    pass


@Progress(prefix="search", suffix="db")
class DefaultDBSearchPolarsCache(DefaultDBPolarsCache):
    pass
