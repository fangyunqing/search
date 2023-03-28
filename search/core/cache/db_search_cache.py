# @Time    : 2023/03/11 16:23
# @Author  : fyq
# @File    : db_search_cache.py
# @Software: PyCharm

__author__ = 'fyq'

from abc import ABCMeta, abstractmethod
from threading import get_ident
from typing import Optional, List, Any

from loguru import logger

from search import dm

import pandas as pd

from search.core.progress import Progress
from search.core.search_context import SearchContext, SearchBuffer


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
                select_exp = search_buffer.search_sql_object.select_exp
                if search_cache_index == 0:
                    if top:
                        select_exp = f"{select_exp} top {search_context.search_object.top}"

                from_exp = " ".join(search_buffer.from_exp_list)
                from_exp = from_exp.format(*[get_ident() for i in range(0, from_exp.count("{}"))])
                sql_list.append(select_exp)
                sql_list.append(",".join(search_buffer.field_list))
                sql_list.append(from_exp)

                tmp_sql_list.append(select_exp)
                tmp_sql_list.append(",".join(search_buffer.tmp_fields) + f" into {tmp_tablename}")
                tmp_sql_list.append(from_exp)

                sql = " ".join(sql_list)
                tmp_sql = " ".join(tmp_sql_list)

                search_df_list: List[pd.DataFrame] = []
                for conn in conn_list:
                    res = self.exec(conn=conn,
                                    search_buffer=search_buffer,
                                    sql=sql,
                                    tmp_sql=tmp_sql)
                    search_df_list.append(pd.DataFrame(data=res, columns=search_buffer.select_fields))
                    if search_cache_index == 0 and top:
                        break

                if data_df is None:
                    if len(search_df_list) > 1:
                        data_df = pd.concat(search_df_list)
                    else:
                        data_df = search_df_list[0]
                else:
                    if len(search_df_list) > 1:
                        new_df = pd.concat(search_df_list)
                        data_df = pd.merge(left=data_df, right=new_df, how="left",
                                           left_on=search_buffer.join_fields, right_on=search_buffer.join_fields)
                    else:
                        new_df = search_df_list[0]
                        data_df = pd.merge(left=data_df, right=new_df, how="left",
                                           left_on=search_buffer.join_fields, right_on=search_buffer.join_fields)

        finally:
            [conn.close() for conn in conn_list]

        return data_df

    @abstractmethod
    def count(self, conn_list: List, search_context: SearchContext, top: bool):
        pass

    @abstractmethod
    def exec(self, conn, search_buffer: SearchBuffer, sql: str, tmp_sql: str) -> Any:
        pass


class DefaultDBCache(AbstractDBSearchCache):
    execs = ["exec"]

    def count(self, conn_list: List, search_context: SearchContext, top: bool):
        c = len(conn_list) * len(search_context.search_buffer_list)
        if top:
            return c - len(conn_list) + 1
        else:
            return c

    def exec(self, conn, search_buffer: SearchBuffer, sql: str, tmp_sql: str) -> Any:
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
