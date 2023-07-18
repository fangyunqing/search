# import abc
# import json
# import typing
#
# import redis
#
# from search.core.search_context import SearchContext
# from search.extend import redis_pool
# from search.generate_key import email_export_notice_key
# from search.util.redis import redis_lock
# import smtplib
#
# NoticeUnit = typing.TypeVar("NoticeUnit")
#
#
# class Notice(typing.Generic[NoticeUnit], metaclass=abc.ABCMeta):
#
#     def __init__(self):
#         self.notice_units: typing.List[NoticeUnit] = []
#
#     @abc.abstractmethod
#     def add(self, search_context: SearchContext, notice_unit: NoticeUnit):
#         pass
#
#     @abc.abstractmethod
#     def delete(self, search_context: SearchContext, notice_unit: NoticeUnit):
#         pass
#
#     @abc.abstractmethod
#     def notice(self, search_context: SearchContext):
#         pass
#
#
# class EmailExportNotice(Notice[str]):
#
#     redis_key = "AB7B780F-F8AB-8D50-71BF-0655DEA2337B"
#
#     def add(self, search_context: SearchContext, notice_unit: str):
#         with redis_lock(key=self.redis_key,
#                         ex=600,
#                         forever=True) as res:
#             if res:
#                 key = email_export_notice_key.generate(search_context=search_context)
#                 r = redis.Redis(connection_pool=redis_pool)
#                 key_value: bytes = r.get(key)
#                 if key_value:
#                     value = key_value.decode()
#                     work_code_list: typing.List[str] = json.loads(value)
#                     work_code_list.append(notice_unit)
#                 r.set(key, json.dumps(work_code_list))
#
#     def delete(self, search_context: SearchContext, notice_unit: str):
#         pass
#
#     def notice(self, search_context: SearchContext):
#         with redis_lock(key=self.redis_key,
#                         ex=600,
#                         forever=True) as res:
#             if res:
#                 key = email_export_notice_key.generate(search_context=search_context)
#                 r = redis.Redis(connection_pool=redis_pool)
#                 key_value: bytes = r.get(key)
#                 if key_value:
#                     value = key_value.decode()
#                     work_code_list: typing.List[str] = json.loads(value)
#                     if len(work_code_list) > 0:
#                         admin_email = smtplib.SMTP_SSL("smtp.mxhichina.com", 465)
#                         admin_email.login("adminemail@huafeng-cn.com", "Huafeng2022")
#
#                         from email.mime.multipart import MIMEMultipart
#                         from email.mime.text import MIMEText
#                         from email.header import Header
#
#                         e = MIMEMultipart()
#                         t = Header("测试", "utf8").encode()
#                         e['Subject'] = t
#                         e['From'] = 'adminemail@huafeng-cn.com'
#                         e['To'] = 'yunqing.fang@huafeng-cn.com'
#                         e.attach(MIMEText('忘记密码需要找回密码', 'plain', 'utf-8'))
#                         admin_email.sendmail('adminemail@huafeng-cn.com',
#                                              '726550822@qq.com',
#                                              e.as_string())
#                         admin_email.quit()
import smtplib

if __name__ == "__main__":
    # search_context = SearchContext()
    # search_context.search_key = 'abc'
    # email_export_notice = EmailExportNotice()
    # email_export_notice.add(search_context, 'fyq')
    # email_export_notice.notice(search_context)
    admin_email = smtplib.SMTP_SSL("smtp.mxhichina.com", 465)
    admin_email.login("adminemail@huafeng-cn.com", "Huafeng2022")

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.header import Header

    e = MIMEMultipart()
    t = Header("测试", "utf8").encode()
    e['Subject'] = t
    e['From'] = 'adminemail@huafeng-cn.com'
    e['To'] = 'yunqing.fang@huafeng-cn.com'
    e.attach(MIMEText('忘记密码需要找回密码', 'plain', 'utf-8'))
    admin_email.sendmail('adminemail@huafeng-cn.com',
                         'yunqing.fang@huafeng-cn.com',
                         e.as_string())