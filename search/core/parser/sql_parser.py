# @Time    : 2023/03/28 9:08
# @Author  : fyq
# @File    : sql_parser.py
# @Software: PyCharm

__author__ = 'fyq'

import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Tuple, Optional, Union

import sqlparse
import sqlparse.keywords
from sqlparse import tokens
from sqlparse.sql import Token, IdentifierList, Identifier, Where, Statement, Comparison, Parenthesis
from sqlparse.tokens import DML, Keyword

from search.exceptions import MustNotHaveSubSelectException

sqlparse.keywords.KEYWORDS["TOP"] = tokens.Keyword
sqlparse.keywords.SQL_REGEX.insert(0, (re.compile(r"(TOP)\s+(\d+)", sqlparse.keywords.FLAGS).match, tokens.Keyword.Top))


@dataclass
class SqlParserInfo:

    # sql
    expression: str = ""

    # select top distinct
    select_expression: str = ""

    # fields
    fields: List[Tuple[str, str]] = field(default_factory=lambda: [])

    # from
    from_expression: str = ""

    # where
    where_expression: str = ""

    # other group by order by
    other_expression: str = ""

    # condition
    conditions: List[Tuple[str, str, str]] = field(default_factory=lambda: [])

    # result
    results: List[Tuple[str, str, str]] = field(default_factory=lambda: [])


class ISearchSqlParser(metaclass=ABCMeta):

    @abstractmethod
    def parse(self, sql: str) -> SqlParserInfo:
        pass


class SearchSqlParser(ISearchSqlParser):

    def parse(self, sql: str) -> SqlParserInfo:

        select_tokens = []
        field_token: Union[IdentifierList, Identifier, None] = None
        from_tokens = []
        where_token = None
        other_tokens = []
        select_pos = None
        from_pos = None
        filed_pos = None

        sql = sqlparse.format(sql, keyword_case="upper", strip_whitespace=True, use_space_around_operators=True)
        statement: Statement = sqlparse.parse(sql)[0]
        all_tokens: List[Token] = statement.tokens

        for token in all_tokens:
            if self.is_sub_select(token):
                raise MustNotHaveSubSelectException

        info = SqlParserInfo()
        info.expression = sqlparse.format(sql, keyword_case="upper", strip_whitespace=True, reindent=True,
                                          use_space_around_operators=True)

        for token_index, token in enumerate(all_tokens):
            if token.ttype is DML and token.value.upper() == "SELECT" and select_pos is None:
                select_pos = token_index
            elif isinstance(token, (Identifier, IdentifierList)) and filed_pos is None:
                select_tokens = all_tokens[select_pos + 1: token_index]
                field_token = token
                filed_pos = token_index
            elif token.ttype is Keyword and token.value.upper() == "FROM" and from_pos is None:
                from_pos = token_index
            elif token.is_group and isinstance(token, Where):
                from_tokens = all_tokens[from_pos + 1: token_index]
                where_token = token
                other_tokens = all_tokens[token_index + 1:]

        info.select_expression = self._parse_tokens(select_tokens).strip()
        if field_token:
            info.fields.extend(self._parse_fields(field_token))
        info.from_expression = self._parse_tokens(from_tokens).strip()

        if where_token:
            where_res = self._parse_where(where_token.tokens, info.conditions, info.results)
            info.where_expression = " ".join(where_res).replace("WHERE", "")

        info.other_expression = self._parse_tokens(other_tokens).strip()

        return info

    @classmethod
    def _parse_tokens(cls, select_tokens: List[Token]):
        exp = ""
        for token in select_tokens:
            exp += token.value
        return exp

    @classmethod
    def _parse_fields(cls, field_token: Union[IdentifierList, Identifier, None]):
        field_list: List[Tuple[str, str]] = []
        if isinstance(field_token, IdentifierList):
            for token in field_token.tokens:
                if isinstance(token, Identifier):
                    value_list = token.value.split()
                    field_list.append((" ".join(value_list[0: -1]), value_list[-1]))
        elif isinstance(field_token, Identifier):
            value_list = field_token.value.split()
            field_list.append((" ".join(value_list[0: -1]), value_list[-1]))
        return field_list

    @classmethod
    def _parse_where(cls, where_tokens: List[Token], conditions: List, results: List):

        res = []

        def _parse_comparison(comparison: Comparison, datas: List):
            if len(comparison.tokens) == 1:
                datas.append(("", "", last_token.value))
            elif len(comparison.tokens) == 3:
                datas.append((comparison.tokens[0].value, "", last_token.value))
            elif len(comparison.tokens) > 3:
                mid = ""
                for mid_t in token[1: -1]:
                    mid += mid_t.value
                datas.append((comparison.tokens[0].value, mid, last_token.value))

        for token in where_tokens:
            if isinstance(token, Parenthesis):
                res += cls._parse_where(token.tokens, conditions, results)
            elif isinstance(token, Comparison):
                if len(token.tokens) > 1:
                    last_token: Token = token.tokens[-1]
                    if last_token.value.startswith("condition."):
                        _parse_comparison(token, conditions)
                    elif last_token.value.startswith("result."):
                        _parse_comparison(token, results)
                res.append(token.value)
            elif not token.is_whitespace:
                res.append(token.value)

        return res

    @classmethod
    def is_sub_select(cls, parsed):
        if not parsed.is_group:
            return False
        if hasattr(parsed, "tokens"):
            for item in parsed.tokens:
                if item.ttype is DML and item.value.upper() == 'SELECT':
                    return True
                else:
                    if cls.is_sub_select(item):
                        return True
            return False
