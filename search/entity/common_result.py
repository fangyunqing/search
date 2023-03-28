# @Time    : 23/02/20 11:56
# @Author  : fyq
# @File    : common_result.py
# @Software: PyCharm

__author__ = 'fyq'

from dataclasses import dataclass, fields

from search.entity import MessageCode


@dataclass
class BaseDataClass:

    def __new__(cls, *args, **kwargs):
        field_list = []
        for field in fields(cls):
            field_list.append(field.name)
        cls.field_list = field_list
        return super(BaseDataClass, cls).__new__(cls)

    def to_dict(self):
        d = {}
        for field_name in self.field_list:
            o = self.__dict__[field_name]
            if hasattr(o, "to_dict"):
                d[field_name] = o.to_dict()
            elif isinstance(o, list):
                d[field_name] = []
                for v in o:
                    if hasattr(v, "to_dict"):
                        d[field_name].append(v.to_dict())
                    else:
                        d[field_name].append(v)
            else:
                d[field_name] = o
        return d


@dataclass
class CommonResult(BaseDataClass):

    message: str = "success"
    data: any = None
    code: int = MessageCode.SUCCESS.code

    @staticmethod
    def success(data: any = None) -> dict:
        return CommonResult(data=data).to_dict()

    @staticmethod
    def fail(code: int, message: str, data: any = None) -> dict:
        return CommonResult(code=code, message=message, data=data).to_dict()
