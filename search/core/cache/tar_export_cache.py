# @Time    : 2023/03/11 15:45
# @Author  : fyq
# @File    : tar_export_cache.py
# @Software: PyCharm

__author__ = 'fyq'

import math
import os
import tarfile
import uuid
from abc import ABCMeta, abstractmethod
from typing import Optional, List

import pandas as pd

from sqlalchemy import desc

from search import constant
from search import models, db
from search.core.decorator import search_cost_time
from search.core.progress import Progress
from search.core.search_context import SearchContext

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


class TarExportCache(metaclass=ABCMeta):

    @abstractmethod
    def get_data(self, search_context: SearchContext) -> Optional[models.SearchFile]:
        pass

    @abstractmethod
    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):
        pass


class AbstractTarExportCache(TarExportCache):

    def get_data(self, search_context: SearchContext) -> Optional[models.SearchFile]:
        search_file: models.SearchFile = (models.SearchFile
                                          .query
                                          .filter_by(search_md5=search_context.search_key, use=constant.EXPORT)
                                          .order_by(desc(models.SearchFile.create_time))
                                          .first())
        return search_file

    @search_cost_time
    def set_data(self, search_context: SearchContext, data_df: pd.DataFrame, file_dir: str):

        def remove(val):
            if isinstance(val, str):
                return (ILLEGAL_CHARACTERS_RE.sub(" ", val)
                        .replace("\n", " ")
                        .replace("\r", " ")
                        .replace("`", " ")
                        )
            return val

        new_data_df = pd.DataFrame()
        for column in search_context.search_md5.search_original_field_list:
            if column in data_df.columns:
                for search_field in search_context.search_field_list:
                    if search_field.name == column:
                        if pd.api.types.is_string_dtype(data_df[column]) \
                                or 'string[pyarrow]' in data_df[column].dtypes.name:
                            new_data_df[search_field.display] = \
                                data_df[column].apply(remove)
                        else:
                            new_data_df[search_field.display] = data_df[column]
                        break
        data_df = new_data_df
        tar_dir: str = file_dir + os.sep + str(uuid.uuid4())
        os.makedirs(tar_dir)
        size = search_context.search.export_single_size
        file_path_list: List[str] = []
        try:
            for index in range(0, self.count(data_df=data_df, size=size)):
                chunk_df: pd.DataFrame = data_df.iloc[index * size:(index + 1) * size]
                if len(chunk_df) > 0:
                    self.exec(chunk_df=chunk_df,
                              search_context=search_context,
                              index=index,
                              tar_dir=tar_dir,
                              file_path_list=file_path_list)
            tar_path = f"{tar_dir}{os.path.sep}{search_context.search.name}.tar.gz"
            self.exec_tar(search_context=search_context,
                          tar_path=tar_path,
                          file_path_list=file_path_list)

            d, f = os.path.split(tar_path)
            search_file = models.SearchFile()
            search_file.path = tar_path
            search_file.file_name = f
            search_file.search_md5 = search_context.search_key
            search_file.use = constant.FileUse.EXPORT
            search_file.size = os.path.getsize(tar_path)
            search_file.search_id = search_context.search.id
            db.session.add(search_file)
            db.session.commit()
        finally:
            for f in file_path_list:
                if os.path.exists(f):
                    os.remove(path=f, dir_fd=None)

    @abstractmethod
    def count(self, data_df: pd.DataFrame, size: int):
        pass

    @abstractmethod
    def exec(self, search_context: SearchContext, chunk_df: pd.DataFrame, index: int, tar_dir: str,
             file_path_list: List[str]):
        pass

    @abstractmethod
    def exec_tar(self, search_context: SearchContext, tar_path: str, file_path_list: List):
        pass


@Progress(prefix="export", suffix="tar")
class DefaultTarExportCache(AbstractTarExportCache):

    def exec_tar(self, search_context: SearchContext, tar_path: str, file_path_list: List):
        with tarfile.open(name=tar_path, mode="w:gz", encoding='utf-8') as tar:
            for file_path in file_path_list:
                d, f = os.path.split(file_path)
                tar.add(file_path, arcname=f)

    execs = ["exec", "exec_tar"]

    def count(self, data_df: pd.DataFrame, size: int):
        return math.ceil(len(data_df) / size) + 1

    def exec(self, search_context: SearchContext, chunk_df: pd.DataFrame, index: int, tar_dir: str,
             file_path_list: List[str]):
        file_path = f"{tar_dir}{os.path.sep}{search_context.search.name}_{index}"
        if constant.FileType.CSV == search_context.search.export_file_type:
            file_path = file_path + ".csv"
            chunk_df.to_csv(file_path, sep="`", index=False, encoding="utf_8_sig")
        else:
            file_path = file_path + ".xlsx"
            chunk_df.to_excel(file_path, index=False)
        file_path_list.append(file_path)
