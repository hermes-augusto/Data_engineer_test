import smtplib, configparser
import sys
import datetime
import email.utils
from pyspark.sql.functions import sum, col, count,when
def envia_mail():
    print(str(datetime.datetime.now())+': Inicia envio de email em caso de haver fraude ')
    config = configparser.ConfigParser()
    config.read('/home/hermes/teste/files/properties.ini')
    gmail_user = config['EMAIL']['user']
    gmail_password = config['EMAIL']['password']
    sent_from = gmail_user
    to = ['hermes.barboza@gmail.com']
    email_text = """
    Atencao!
    Possivel fraude analise base XPT no hive!
    """
    SUBJECT = 'ALERTA'
    message = 'Subject: {} {}'.format(SUBJECT, email_text)
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.sendmail(sent_from, to, message)
        server.close()
        print(str(datetime.datetime.now())+': Enviado com sucesso!')
    except Exception as e:
        print(e)
        print ('Opa algo deu errado...')

def valida_email(df):
    ff = df.filter(col('Alerta') == 'Fraude')
    if(len(ff.head(1)) != 0):
        return True
    return False
