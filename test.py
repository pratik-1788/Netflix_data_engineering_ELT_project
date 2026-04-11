from airflow.utils.email import send_email

send_email(
    to="pratikchandel788@gmail.com",
    subject="Test Email",
    html_content="It works!"
)


# Conn Id: smtp_default
# Conn Type: Email
# Host: smtp.gmail.com
# Login: pratikchandel788@gmail.com
# Password: gprjvqrmdsnwjtob
# Port: 587
# {
#   "smtp_starttls": true,
#   "smtp_ssl": false
# }