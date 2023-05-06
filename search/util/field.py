# @Time    : 2023/05/04 10:17
# @Author  : fyq
# @File    : field.py
# @Software: PyCharm

__author__ = 'fyq'

import polars as pl
from typing import Optional

_uuid = "u"
_int = "i"
_float = "n"
_str = "s"
_datetime = "t"
_date = "d"
_bool = "b"


def get_pl_expr(field_name: str) -> Optional[pl.Expr]:
    if field_name.startswith(_int):
        return pl.col(field_name).cast(pl.Int32)
    elif field_name.startswith(_float):
        return pl.col(field_name).cast(pl.Decimal(precision=20, scale=8))
    elif field_name.startswith(_uuid):
        return pl.col(field_name).apply(lambda x: str(x)).cast(pl.Utf8)
    elif field_name.startswith(_str):
        return pl.col(field_name).cast(pl.Utf8)
    elif field_name.startswith(_datetime):
        return pl.col(field_name).cast(pl.Datetime)
    elif field_name.startswith(_date):
        return pl.col(field_name).cast(pl.Date)
    elif field_name.startswith(_bool):
        return pl.col(field_name).cast(pl.Boolean)


def get_sql_field_type(field_name: str) -> Optional[str]:
    if field_name.startswith(_int):
        return f"{field_name} int"
    elif field_name.startswith(_float):
        return f"{field_name} decimal(18, 5)"
    elif field_name.startswith(_uuid):
        return f"{field_name} uniqueidentifier"
    elif field_name.startswith(_str):
        return f"{field_name} nvarchar(128)"
    elif field_name.startswith(_datetime):
        return f"{field_name} datetime"
    elif field_name.startswith(_date):
        return f"{field_name} date"
    elif field_name.startswith(_bool):
        return f"{field_name} bit"


def right_filed_name(field_name: str) -> bool:
    pass
