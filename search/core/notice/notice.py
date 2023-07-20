import abc
import datetime
import json
import typing
import uuid

import redis
import tabulate

from search.basicdata import insert_message_notice
from search.core.search_context import SearchContext
from search.entity.basic import MessageNotice
from search.extend import redis_pool
from search.generate_key import export_tar_notice_key
from search.util.redis import redis_lock

NoticeUnit = typing.TypeVar("NoticeUnit")
tabulate.WIDE_CHARS_MODE = True


class Notice(typing.Generic[NoticeUnit], metaclass=abc.ABCMeta):

    def __init__(self):
        self.notice_units: typing.List[NoticeUnit] = []

    @abc.abstractmethod
    def add(self, search_context: SearchContext, notice_unit: NoticeUnit):
        pass

    @abc.abstractmethod
    def delete(self, search_context: SearchContext, notice_unit: NoticeUnit):
        pass

    @abc.abstractmethod
    def notice(self, search_context: SearchContext):
        pass


class ExportTarNotice(Notice[str]):
    redis_key = "AB7B780F-F8AB-8D50-71BF-0655DEA2337B"

    def add(self, search_context: SearchContext, notice_unit: str):
        with redis_lock(key=self.redis_key,
                        ex=600,
                        forever=True) as res:
            if res:
                key = export_tar_notice_key.generate(search_context=search_context)
                r = redis.Redis(connection_pool=redis_pool)
                key_value: bytes = r.get(key)
                if key_value:
                    value = key_value.decode()
                    user_id_list: typing.List[str] = json.loads(value)
                    if notice_unit not in user_id_list:
                        user_id_list.append(notice_unit.strip())
                        r.setex(name=key, time=1800, value=json.dumps(user_id_list))
                else:
                    r.setex(name=key, time=1800, value=json.dumps([notice_unit.strip()]))

    def delete(self, search_context: SearchContext, notice_unit: str):
        pass

    def notice(self, search_context: SearchContext):
        with redis_lock(key=self.redis_key,
                        ex=600,
                        forever=True) as res:
            if res:
                key = export_tar_notice_key.generate(search_context=search_context)
                r = redis.Redis(connection_pool=redis_pool)
                key_value: bytes = r.get(key)
                if key_value:
                    value = key_value.decode()
                    user_id_list: typing.List[str] = json.loads(value)
                    if len(user_id_list) > 0:
                        text_condition_list = []
                        for k, v in search_context.search_md5.search_conditions.items():
                            name = k
                            if k in search_context.search_condition_dict:
                                name = search_context.search_condition_dict[k].display
                            text_condition_list.append([name, v])
                        table_data = tabulate.tabulate(tabular_data=text_condition_list,
                                                       headers=["条件", "值"],
                                                       tablefmt='simple',
                                                       numalign='center',
                                                       stralign='center')

                        message_notice = MessageNotice()
                        message_notice.message_items_guid = "00000000-0000-0000-0000-000000000000"
                        message_notice.sender = 'cttsoft'
                        message_notice.send_time = datetime.datetime.now()
                        message_notice.title = f"{search_context.search.display}打包完成，可以进行导出"
                        message_notice.content = str(table_data).replace("\n", "\r\n")
                        message_notice.send_type = "SYS"
                        message_notice.show_type = "BAR;WIN"
                        message_notice.msg_type = 0
                        message_notice.read = 0

                        for user_id in user_id_list:
                            message_notice.guid = str(uuid.uuid1()).upper()
                            message_notice.acceptor = user_id
                            insert_message_notice(message_notice=message_notice)


export_tar_notice = ExportTarNotice()
