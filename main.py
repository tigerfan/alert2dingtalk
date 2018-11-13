import time
import pyodbc
import json
import logging
import requests
from dingtalkchatbot.chatbot import DingtalkChatbot, ActionCard, FeedLink, CardItem

webhook = 'https://oapi.dingtalk.com/robot/send?access_token=xxx'
xiaoding = DingtalkChatbot(webhook)

conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=xxx\SQLEXPRESS;DATABASE=xxx;UID=xxx;PWD=xxx') 
cursor = conn.cursor()
cursor.execute("SELECT TOP 10 * FROM dbo.ALARM_LOG WHERE dbo.ALARM_LOG.sequence_number < 10000 ORDER BY dbo.ALARM_LOG.sequence_number DESC")
news = cursor.fetchall()
current_number = news[9].sequence_number
print (current_number)

while 1:
    cursor.execute("SELECT dbo.ALARM_LOG.sequence_number, dbo.ALARM_LOG.alarm_class, dbo.ALARM_LOG.alarm_message, dbo.ALARM_LOG.TIMESTAMP FROM dbo.ALARM_LOG WHERE dbo.ALARM_LOG.sequence_number =? ORDER BY dbo.ALARM_LOG.sequence_number DESC", current_number)
    row = cursor.fetchone()
    if not row:
        print (current_number)
        continue
    else:
        alert_msg = '### 报警信息：\n > %s \n\n > %s \n\n > ![alert](http://uc-test-manage-00.umlife.net/jenkins/pic/coverage.png)\n' %(row.alarm_message, row.TIMESTAMP)
        xiaoding.send_markdown(title='报警信息', text=alert_msg, is_at_all=True)
        print(row.sequence_number, row.alarm_class, row.alarm_message, row.TIMESTAMP)
        current_number += 1
    time.sleep(300)

cursor.close()
conn.close()
