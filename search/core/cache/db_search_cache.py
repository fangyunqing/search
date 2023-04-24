# @Time    : 2023/03/11 16:23
# @Author  : fyq
# @File    : db_search_cache.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from threading import get_ident
from typing import Optional, List, Any

import munch
from loguru import logger

from search import dm

import pandas as pd

from search.core.progress import Progress
from search.core.search_context import SearchContext, SearchBuffer

from pyext import RuntimeModule

from search.util.bytes import format_bytes


class DBSearchCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[pd.DataFrame]:
        pass


class AbstractDBSearchCache(DBSearchCache):

    def get_data(self, search_context: SearchContext, top: bool = False) -> Optional[pd.DataFrame]:
        conn_list = dm.get_connections()
        self.count(search_context=search_context, conn_list=conn_list, top=top)
        data_df = None
        try:
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

                data = []
                for conn in conn_list:
                    res = self.exec(conn=conn,
                                    search_context=search_context,
                                    search_buffer=search_buffer,
                                    sql=sql,
                                    tmp_sql=tmp_sql)
                    if res:
                        data.extend(res)
                    if search_cache_index == 0 and top:
                        break

                if len(data) > 0:
                    new_df = pd.DataFrame(data=data, columns=search_buffer.select_fields)
                    del data
                    if data_df is None:
                        data_df = new_df
                    else:
                        data_df = data_df.merge(right=new_df,
                                                how=search_buffer.search_sql.how,
                                                on=search_buffer.join_fields)
                        del new_df
                    logger.info(f"内存:{format_bytes(data_df.memory_usage(deep=True).sum())}")
        finally:
            [conn.close() for conn in conn_list]

        return self.exec_new_df(search_context=search_context,
                                df=data_df)

    @abstractmethod
    def count(self, conn_list: List, search_context: SearchContext, top: bool):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, search_buffer: munch.Munch, conn, sql: str, tmp_sql: str) -> Any:
        pass

    @abstractmethod
    def exec_new_df(self, search_context: SearchContext, df: pd.DataFrame) -> pd.DataFrame:
        pass


class DefaultDBCache(AbstractDBSearchCache):

    def exec_new_df(self, search_context: SearchContext, df: pd.DataFrame) -> pd.DataFrame:
        search_field_dict = {search_field.name: search_field
                             for search_field in search_context.search_field_list}
        new_df = pd.DataFrame(columns=search_context.search_md5.search_original_field_list)
        for field in search_context.search_md5.search_original_field_list:
            if field in search_field_dict:
                search_field = search_field_dict[field]
                try:
                    if search_field.rule.startswith(("def", "import", "from")):
                        md = RuntimeModule.from_string('a', search_field.rule)
                        find = False
                        for v in md.__dict__.values():
                            if callable(v):
                                new_df[search_field.name] = df.apply(v, axis=1)
                                find = True
                                break
                        if not find:
                            new_df[search_field.name] = None
                            logger.warning(f"{search_context.search_md5.search_name}-{search_field.name}-rule未发现可执行函数")
                    else:
                        if search_field.rule in df:
                            new_df[search_field.name] = df[search_field.rule]
                        else:
                            new_df[search_field.name] = None
                            logger.warning(f"查询结果中未包含列[{search_field.name}]")
                except Exception as e:
                    new_df[search_field.name] = None
                    logger.exception(e)
            else:
                new_df[field] = None

        return new_df

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
            return cur.fetchall()
        except Exception as e:
            if hasattr(e, "args") and e.args[0] != 208:
                raise e
            else:
                logger.warning(e)
        finally:
            cur.close()


@Progress(prefix="export", suffix="db")
class DefaultDBExportCache(DefaultDBCache):
    pass


@Progress(prefix="search", suffix="db")
class DefaultDBSearchCache(DefaultDBCache):
    pass
