#!/usr/bin/python
# -*- coding: utf-8 -*-

from smtplib import SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


class QQMail:
    def __init__(self,smtp_sender,smtp_passwd,
                 smtp_receiver):
        self.smtp_server_host = 'smtp.qq.com'
        self.smtp_sender = smtp_sender
        self.smtp_passwd = smtp_passwd
        self.smtp_receiver = smtp_receiver
    def login(self):
        self.smtp = SMTP_SSL(self.smtp_server_host)
        try:
            self.smtp.login(self.smtp_sender,self.smtp_passwd)
            return True
        except:
            print("登录邮箱服务器失败！")
            return False
        
    def makeHeader(self,subject,sender_nick_name):
        self.msg = MIMEMultipart()
        self.msg["Subject"] = Header(subject,'utf-8')
        self.nickName = Header(sender_nick_name,'utf-8').encode()
        self.msg["From"] = f"{self.nickName} <{self.smtp_sender}>"
        self.msg['To'] = ";".join(self.smtp_receiver)
    
    def makeText(self,content,type="plain"):
        self.msg.attach(MIMEText(content,type,'utf-8'))

    def makeHtml(self,content):
        self.makeText(content,type="html")

    # def addUploadFile(self,file_name,file_path):
    #     attachment=MIMEApplication(open(file_path,'rb').read())
    #     #为附件添加一个标题
    #     attachment.add_header('Content-Disposition','attachment',filename=file_name)
    #     self.msg.attach(attachment)

    def send(self):
        try:
            self.smtp.sendmail(self.smtp_sender,self.smtp_receiver,self.msg.as_string())
            self.smtp.quit()
            return True
        except:
            print("发送邮件失败！")
            return False

def send_mail(receivers, header, sub_header, content):
    if len(receivers) <= 0:
        return
       
    qq = QQMail('672817221@qq.com', 'ugwkzioulsvbbebg', receivers)
    if qq.login():
        qq.makeHeader(header, sub_header)
        qq.makeHtml(content)
        qq.send()
