# -*- coding: utf-8 -*-

#import sys
import time
import datetime
import pyodbc
import json
import logging
import requests
from dingtalkchatbot.chatbot import DingtalkChatbot, ActionCard, FeedLink, CardItem

#关键词筛选，严重故障过滤后直接推送报警
#alertlist_0 = ('报警', '运行', '停止', '熄火', '过载', '火灾', '堵车', '断路器', '变频器')
alertlist_0 = ('熄火', '过载', '火灾', '堵车', '断路器', '变频器')
alertlist_1 = ('面涂空调故障', '面涂静电高压故障', '面涂旋杯停止故障', '面涂火灾警报')
alertlist_2 = ('输送系统故障', '机运故障', 'PLC故障', '输送链故障', '机运系统故障')
alertlist = alertlist_0 + alertlist_1 + alertlist_2
theyare = []
thendid = []

#停止和运行报警过滤
runningflag = 'RUNNING'
stopflag = 'STOP'
whois = []
whenhappen = []

#机器人急停报警过滤
haltflag = '机器人急停'
lasthalt = datetime.datetime(2011,12,31,0,0,0)

#过滤时间参数
dly_rpt = 600       #10分钟
dly_hlt = 3600      #1小时
dly_rst = 30        #半小时

#钉钉机器人
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=xxx'
xiaoding = DingtalkChatbot(webhook)

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=SERVER4;DATABASE=pmc;UID=sa;PWD=xxx') 
cursor = conn.cursor()
cursor.execute("SELECT TOP 3 * FROM dbo.ALARM_LOG ORDER BY sequence_number DESC")
news = cursor.fetchall()

current_number = news[2].sequence_number
#current_number = 180000

while 1:
    cursor.execute("SELECT sequence_number, alarm_id, alarm_message, TIMESTAMP FROM dbo.ALARM_LOG WHERE sequence_number =? ORDER BY sequence_number DESC", current_number)
    row = cursor.fetchone()

    if not row:
        continue
    else:
        ts = row.TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")
        k = len(row.alarm_id)

        for alist in alertlist:
            if alist in row.alarm_message:
                if len(theyare) > 0 and row.alarm_id in theyare:
                    i = theyare.index(row.alarm_id)
                    j = round((row.TIMESTAMP - thendid[i]).total_seconds())
                    if j > dly_rpt:
                        alert_msg = '### 报警信息：\n > %s \n\n > %s \n\n' %(ts, row.alarm_message[k:])
                        xiaoding.send_markdown(title='报警信息', text=alert_msg, is_at_all=True)
                        print(ts, row.alarm_message[k:])                                           
                    thendid[i] = row.TIMESTAMP
                else:
                    theyare.append(row.alarm_id)
                    thendid.append(row.TIMESTAMP)
                break

        if haltflag in row.alarm_message and (row.TIMESTAMP - lasthalt).total_seconds() > dly_hlt:
            alert_msg = '### 报警信息：\n > %s \n\n > %s \n\n' %(ts, row.alarm_message[k:])
            xiaoding.send_markdown(title='报警信息', text=alert_msg, is_at_all=True)
            print(ts, row.alarm_message[k:])
            lasthalt = row.TIMESTAMP

        if stopflag in row.alarm_id and row.alarm_id[-5] == '.':
            if len(whois) > 0 and row.alarm_id[:-4] in whois:
                print('---重复的停止信号---')
            else:
                whois.append(row.alarm_id[:-4])
                whenhappen.append(row.TIMESTAMP)
        elif runningflag in row.alarm_id and row.alarm_id[-8] == '.':
            if len(whois) > 0 and row.alarm_id[:-7] in whois:
                i = whois.index(row.alarm_id[:-7])
                j = round((row.TIMESTAMP - whenhappen[i]).total_seconds()/60)
                if j > dly_rst:
                    alert_msg = '### 报警信息：\n > %s \n\n > %s 之前停运 %s 分钟！\n\n' %(ts, row.alarm_message[k:], j)
                    xiaoding.send_markdown(title='报警信息', text=alert_msg, is_at_all=True)
                    print(ts, row.alarm_message[k:], '之前停运', j, '分钟！')
                del whois[i]
                del whenhappen[i]
            else:
                print(ts, row.alarm_message[k:])

        current_number += 1

cursor.close()
conn.close()  
  
