import os
import re
import psutil
import psycopg2
from psycopg2.extras import DictCursor
import sendgrid
from twilio.rest import Client
from sendgrid.helpers.mail import *

my_twilio_phone_number = "+18333655808"

# source ./sendgrid.env
sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
sg = sendgrid.SendGridAPIClient(sendgrid_api_key) # Sendgrid email client

# source ./sendgrid.env
account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN")
sms_client = Client(account_sid, auth_token) # Twilio sms client


db_params = {
    "dbname": "scadabase",
    "user": "postgres",
    "password": "hydrogen",
    "host": "127.0.0.1",
    "port": "5432"
}
conn = psycopg2.connect(**db_params, cursor_factory = DictCursor)
conn.autocommit = True

def main():
    try:
        records = fetch_records()
        for record in records:
            result = process_record(record)
            if not result:
                print("Skipping")
                continue
            archive_record(record)
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
    FROM mail."MailQueue"
    WHERE "deliveryMethod" IS NULL FOR UPDATE;
    """
    rows = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.close()
    return rows


def process_record(record):
    destination = record["DestinationAddress"]
    target = destination.strip().split('@')[0]
    if len(target) < 1:
        print(f"Invalid destination address: {destination}")
        return False # Invalid address

    result = None
    if re.search("^\\d{10}",target): # assume phone number
        result = send_sms(record)
    else:                    # assume email
        result = send_email(record)
    return result


def send_sms(record):
    try:
        target_phone_number = record["DestinationAddress"].strip().split('@')[0]
        message = sms_client.messages.create(
            to = target_phone_number,  # Replace with the recipient"s phone number
            from_ = my_twilio_phone_number,  # Replace with your Twilio phone number
            body = record["Body"],
        )
    except Exception as e:
        print(f"Error sending sms: {e}")
        return None
    return True


def send_email(record):
    try:
        mail = Mail(
            from_email = record["SourceAddress"],
            subject = record["Subject"],
            html_content = record["Body"]
        )

        personalization = Personalization()
        personalization.add_to(To(record["DestinationAddress"]))

        if record["CC_Address"] != None and len(record["CC_Address"].strip().split(',')) > 0:
            list = record["CC_Address"].strip().split(',')
            for cc in list:
                val = cc.strip()
                if len(val) < 6: continue # a@b.me -> anything shorter is prob bogus
                personalization.add_cc(Cc(val))

        if record["BCC_Address"] != None and len(record["BCC_Address"].strip().split(',')) > 0:
            list = record["BCC_Address"].strip().split(',')
            for bcc in list:
                val = bcc.strip()
                if len(val) < 6: continue # a@b.me -> anything shorter is prob bogus
                personalization.add_bcc(Bcc(val))

        mail.add_personalization(personalization)
        mail_json = mail.get()
        response = sg.client.mail.send.post(request_body = mail_json)
        print(response.status_code)
        if response.status_code < 200 or response.status_code > 204:
            print("Request failed")
            return False
    except Exception as e:
        print(f"Error sending email: {e}")
        return False
    return True


def archive_record(record):
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
            sql = 'DELETE FROM mail."MailQueue" WHERE "ID" = %s'
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
        if process.info["name"] == __file__ and process.info["pid"] != pid:
            print(f"{__file__} is already running.")
            return False
    return True


if __name__ == '__main__' and running_process_check():
    main()