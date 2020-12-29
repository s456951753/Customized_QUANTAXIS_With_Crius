import smtplib, ssl
import AY.Crius.Utils.configuration_file_service as config_service

'''context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email_1, message)
'''


def send_mail(message, receipents=None):
    receiver_email_1 = "austinyxh@hotmail.com"
    receiver_email_2 = "ee07b238@gmail.com"
    acct = config_service.getProperty(section_name=config_service.GMAIL_SECTION_NAME,
                                      property_name=config_service.GM_ACCOUNT_NAME)
    password = config_service.getProperty(section_name=config_service.GMAIL_SECTION_NAME,
                                          property_name=config_service.GM_PASSWORD_NAME)
    server_name = config_service.getProperty(section_name=config_service.GMAIL_SECTION_NAME,
                                             property_name=config_service.GM_SERVER_DNS_NAME)
    server_port = config_service.getProperty(section_name=config_service.GMAIL_SECTION_NAME,
                                             property_name=config_service.GM_SERVER_SSL_SMTP_PORT_NAME)
    acct = 's456951753@gmail.com'
    password = 'xiaobu513'
    server_name = 'smtp.gmail.com'
    server_port = 465
    context = ssl.create_default_context()
    server = smtplib.SMTP_SSL(host=server_name, port=server_port, context=context)
    server.login(acct, password)
    server.sendmail(acct, receiver_email_1, message)
    server.sendmail(acct, receiver_email_2, message)
