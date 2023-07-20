# @Time    : 2023/07/19 9:16
# @Author  : fyq
# @File    : basic.py
# @Software: PyCharm

__author__ = 'fyq'

from dataclasses import dataclass
from datetime import datetime


@dataclass
class User:
    user_id: str = ''

    user_name: str = ''

    email: str = ''

    mobile_no: str = ''

    time_card_id: str = ''


@dataclass
class MessageNotice:
    """
    INSERT INTO smMsgReceviceBox
(uGuid,usmMsgItemsGUID,usmMsgSendBoxGuid,sSender,sAcceptor,tAcceptTime,tSendTime,bReaded,sTitle,sContent,sSendType,
sShowType,iMsgType,tRemindTime,iInterval,usmMsgReceviceBoxGUIDReply,sBody)
VALUES
(NEWID(),'00000000-0000-0000-0000-000000000000',NULL,'cttsoft',@sUserID,NULL,GETDATE(),0,@sBillNo,'出纳收款单:'+@sBillNo+'        客户名称:'+ISNULL(@sBankCustomerName,''),'SYS','BAR;WIN',0,NULL,NULL,NULL,NULL)
END
    """

    guid: str = ''

    message_items_guid: str = ''

    sender: str = ''

    acceptor: str = ''

    send_time: datetime = None

    read: int = 0

    title: str = ''

    content: str = ''

    send_type: str = ''

    show_type: str = ''

    msg_type: int = 0
