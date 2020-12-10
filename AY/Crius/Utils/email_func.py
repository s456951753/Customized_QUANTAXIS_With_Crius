import smtplib, ssl
import Utils.configuration_file_service as config_service

#Gmail password saved locally
password = config_service.getProperty(section_name=config_service.GMAIL_SECTION_NAME,
                                   property_name=config_service.GM_PASSWORD_NAME)


port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "austin.yun365@gmail.com"  # Enter your address

receiver_email_1 = "austinyxh@hotmail.com"
receiver_email_2 = "ee07b238@gmail.com"


message = """\
Subject: Crius update

Hi Yuan, 

Password is now protected. Please use below to update your local properties section. 

Thanks

Crius autotrade terminal

"""

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email_1, message)

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email_2, message)