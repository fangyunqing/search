# @Time    : 2023/07/19 9:14
# @Author  : fyq
# @File    : __init__.py.py
# @Software: PyCharm

__author__ = 'fyq'

from search import dm
from search.entity.basic import User, MessageNotice


def get_user(user_id: str) -> User:
    conn = dm.get_main_connection()
    cursor = conn.cursor()
    try:
        sql = ("SELECT "
               "sUserName, sUserId, sMobileNo, sEmail, sTimeCardID "
               "FROM smUser WHERE sUserId = ?")
        cursor.execute(sql, user_id)
        res = cursor.fetchone()
        if res:
            user = User()
            user.user_id = res[1]
            user.user_name = res[0]
            user.email = res[3]
            user.mobile_no = res[2]
            user.time_card_id = res[4]
            return user
    finally:
        cursor.close()
        conn.close()


def insert_message_notice(message_notice: MessageNotice):
    conn = dm.get_main_connection()
    cursor = conn.cursor()
    try:
        sql = ("INSERT INTO smMsgReceviceBox"
               "(uGuid,usmMsgItemsGUID,sSender,sAcceptor,"
               "tSendTime,bReaded,sTitle,sContent,"
               "sSendType,sShowType,iMsgType)"
               "VALUES("
               "?, ?, ?, ?,"
               "?, ?, ?, ?,"
               "?, ?, ?"
               ")")
        args = (
            message_notice.guid,
            message_notice.message_items_guid,
            message_notice.sender,
            message_notice.acceptor,
            message_notice.send_time,
            message_notice.read,
            message_notice.title,
            message_notice.content,
            message_notice.send_type,
            message_notice.show_type,
            message_notice.msg_type,



        )
        cursor.execute(sql, args)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
