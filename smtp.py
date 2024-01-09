import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from getpass import getpass


sender_email = 'shivukaraguppi1999@gmail.com'
receiver_email = 'aartib@boltinc.com'
subject = 'Trial to send message'
message = 'Hello, This is shivanandk & this mail is sent from python code to check it works or not ###do ignore this msg ###.' #jqvljocuqduvvqpp

msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = subject
msg.attach(MIMEText(message, 'plain'))

try:
    server = smtplib.SMTP('smtp.gmail.com', 587)#jqvljocuqduvvqpp
    server.starttls()
    server.login(sender_email, 'jqvljocuqduvvqpp')
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
    print('Email sent successfully!')
except Exception as e:
    print('Email could not be sent. Error:', str(e))




