import requests

mailurl = "https://emailsender.catax.me/sendEmail"



credemtials_data = {
    "username": "AKIAVG3KVGIQ5K5C54EV",
    "password": "BGI30r7ViaHz5pMhtMjkqw/GDeAD4S3McLoMJltIaaqF",
    "server_addr": "email-smtp.eu-north-1.amazonaws.com",
    "server_port": "587",
    "destination_email": "gewgawrav@gmail.com",
    "sender_email": "error@catax.me",
    "subject": "Test Email",
    "body": "This is a test email. Hello from Error!"
}

response = requests.post(mailurl, json=credemtials_data)

print("Response:")
print(response.status_code)
print(response.text)


# import requests


# def send_mail(mail_from, password, mail_to, subject, content, subtype=None):

#     # The sender interface address to be called, for example http://192.168.1.11:8888/mail_sys/send_mail_http.json
#     url = 'http://164.52.203.21:7800/mail_sys/send_mail_http.json'

#     pdata = {}
#     pdata['mail_from'] = mail_from
#     pdata['password'] = password
#     pdata['mail_to'] = mail_to
#     pdata['subject'] = subject
#     pdata['content'] = content
#     pdata['subtype'] = subtype

#     resp_data = requests.post(url, json=pdata)
#     print(resp_data.text)


# if __name__ == '__main__':

#     # Sender's email address, such as jack@bt.cn
#     mail_from = 'hello@htt.lat'
#     # Sender email address password
#     password = 'Password707*'
#     # Recipient address, separated by commas, such as jack@bt.cn, rose@bt.cn
#     mail_to = 'gewgawrav@gmail.com'
#     # Mail title
#     subject = 'hollywood perfect'
#     # Content of email
#     content = 'song'
#     # Mail type, do not pass the default is plain, to send html please send html
#     subtype = ''
#     send_mail(mail_from, password, mail_to, subject, content)
