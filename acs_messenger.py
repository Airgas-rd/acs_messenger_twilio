import os
import re
import psutil
import psycopg2
from psycopg2.extras import DictCursor
import sendgrid
from twilio.rest import Client
from sendgrid.helpers.mail import *


# Use environment variables later
# sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
# account_sid = os.environ.get("TWILIO_ACCOUNT_SID") # Replace with your Account SID
# auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN") # Replace with your auth token


sendgrid_api_key = "xxxxx"
account_sid = "sdfghj" # Replace with your Account SID
auth_token = "ertyuio" # Replace with your auth token

sms_client = Client(account_sid, auth_token) # Twilio sms client
sg = sendgrid.SendGridAPIClient(sendgrid_api_key) # Twilio sendgrid email client

db_params = {
    "dbname": "scadabase",
    "user": "adminx",
    "password": "hydrogen",
    "host": "127.0.0.1",
    "port": "5432"
}
conn = None

def main():
    try:
        conn = psycopg2.connect(**db_params, cursor_factory = DictCursor)
        records = fetch_records()
        for record in records:
            result =  process_record(record)
            if not result: continue
            archive_record(**record)
    except psycopg2.Error as e:
        print(f"Database Error: {e}")
    finally:
        if conn != None:
            conn.close()


def fetch_records():
    sql = """
    SELECT
        "ID",
        "DestinationAddress",
        "SourceAddress",
        "CC_Address",
        "BCC_Address",
        "Subject",
        "Body",
        "Attachment",
        "attempts"
    FROM mail."MailArchive"
    WHERE "deliveryMethod" IS NULL FOR UPDATE;
    """
    rows = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.close()
    return rows


def process_record(**record):
    destination = record["DestinationAddress"]
    target = destination.strip().split('@')[0]
    if len(target) < 1:
        print(f"Invalid destination address: {destination}")
        return False # Invalid address

    result = None
    if re.search("^\\d{10}"): # assume phone number
        result = send_sms(record)
    else:                    # assume email
        result = send_email(record)
    return result


def send_sms(**data):
    try:
        message = sms_client.messages.create(
            to = data["destination"],  # Replace with the recipient"s phone number
            from_ = data["source"],  # Replace with your Twilio phone number
            body = data["message"],
        )
        print(message.sid)
    except Exception as e:
        print(f"Error sending sms: {e}")
        return None
    return True


def send_email(**data):
    try:
        from_email = Email(data["source"])
        to_email = To(data["destination"])
        subject = data["subject"]
        content = Content("text/plain", data["message"])
        mail = Mail(from_email, to_email, subject, content)
        mail_json = mail.get()
        response = sg.client.mail.send.post(request_body = mail_json)
        print(response.status_code)
        print(response.body)
        print(response.headers)
        if response.status_code != 200:
            return False
    except Exception as e:
        print(f"Error sending email: {e}")
        return False
    return True


def archive_record(**record):
    id = record["ID"]
    source = record["SourceAddress"]
    destination = record["DestinationAddress"]
    cc = record["CC_Address"]
    bcc = record["BCC_Address"]
    subject = record["Subject"]
    body = record["Body"]
    attachment = record["Attachment"]

    try:
        with conn.cursor() as cur:
            sql = 'DELETE FROM mail."MailQueue" WHERE "ID" = %s;'
            params = (id,)
            cur.execute(sql,params)

            sql = '''
            INSERT INTO mail."MailArchive"
                ("ID","SourceAddress","DestinationAddress","CC_Address","BCC_Address","Subject","Body","Attachment","DateSent")
            VALUES
                (DEFAULT,%s,%s,%s,%s,%s,%s,%s,NOW());
            '''
            params = (source,destination,cc,bcc,subject,body,attachment)
            cur.execute(sql,params)
    except psycopg2.Error as e:
        print(f"Database Error: {e}")
        conn.close()


def running_process_check():
    pid = os.getpid()
    for process in psutil.process_iter(["pid","name"]):
        if process.info("name") == __file__ and process.info["pid"] != pid:
            print(f"{__file__} is already running.")
            return False
    return True


if __name__ == '__main__' and not running_process_check():
    main()