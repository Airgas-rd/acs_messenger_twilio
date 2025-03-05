import os
import re
import psutil
import getopt
import sys
import psycopg2
import sendgrid
import pprint
from psycopg2.extras import DictCursor
from twilio.rest import Client
from sendgrid.helpers.mail import *

my_twilio_phone_number = "+18333655808"

#source ./sendgrid.env
sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN")

sg = sendgrid.SendGridAPIClient(sendgrid_api_key) # Sendgrid email client
sms_client = Client(account_sid, auth_token) # Twilio sms client


db_params = {
    "dbname": "scadabase",
    "user": "postgres",
    "password": "hydrogen",
    "host": "127.0.0.1",
    "port": "5432"
}
conn = psycopg2.connect(**db_params, cursor_factory = DictCursor)

help = False
debug = False
testing = False
nonotify = False
email_override = None
phone_override = None

def main():
    try:
        # Get options and arguments
        opts, args = getopt.getopt(sys.argv[1:], "hdtne:p:", ["help", "debug", "testing","no-notify","email=","phone="])
        for opt, arg in opts:
            if opt in ["-h","--help"]:
                global help
                help = True
            elif opt in ["-d","--debug"]:
                global debug
                debug = True
            elif opt in ["-t","--testing"]:
                global testing
                testing = True
            elif opt in ["-n","--no-notify"]:
                global nonotify
                nonotify = True
            elif opt.strip() in ["-e","--email"]:
                global email_override
                email_override = arg
            elif opt.strip() in ["-p","--phone"]:
                global phone_override
                phone_override = arg
        if help == True:
            usage()
            sys.exit(0)
        records = fetch_records()
        for record in records:
            result = process_record(record)
            archive_record(record,result)
    except getopt.GetoptError as e:
        print(e)
        usage()
        sys.exit(2)
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
    if debug == True:
        print(sql)
    rows = []
    try:
        cur = conn.cursor()
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
    if len(destination) < 6 or len(target) < 1: # a@b.me -> anything shorter is prob bogus
        print(f"Invalid destination address: {destination}")
        return False
    result = None
    if re.search("^\\d{10}",target): # assume phone number
        result = send_sms(record)
    else:                            # assume email
        result = send_email(record)
    return result


def send_sms(record):
    try:
        if phone_override != None:
            record["DestinationAddress"] = phone_override
        target_phone_number = record["DestinationAddress"].strip().split('@')[0]
        body = record["Body"]
        if nonotify == True:
            print(f"Notifications disabled. No messages will be sent to {target_phone_number}")
            return True # pretend like it worked
        message = sms_client.messages.create(
            to = target_phone_number,  # Replace with the recipient"s phone number
            from_ = my_twilio_phone_number,  # Replace with your Twilio phone number
            body = body,
        )
        if debug == True:
            print(f"Message to {target_phone_number}")
            print(f"Body: {body}")
            print(f"Status: {message.status}")
        if message.error_code:
            raise Exception(f"SMS error {message.error_code} {message.error_message}")
    except Exception as e:
        print(e)
        return False
    return True


def send_email(record):
    try:
        if email_override:
            record["DestinationAddress"] = email_override
        recipient = record["DestinationAddress"]
        if nonotify == True:
            print(f"Notifications disabled. No messages will be sent to {email_override}")
            return True # pretend like it worked
        mail = Mail(
            from_email = record["SourceAddress"],
            subject = record["Subject"],
            html_content = record["Body"]
        )
        personalization = Personalization()
        personalization.add_to(To(recipient))
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
        response = sg.client.mail.send.post(request_body = mail.get())
        if debug == True:
            print("Email Payload")
            pprint.pprint(mail.get(),indent=4)
            print(f"Email response code: {response.status_code}")
        if response.status_code < 200 or response.status_code > 204:
            raise Exception(f"Email request failed with code {response.status_code}")
    except Exception as e:
        print(e)
        return False
    return True


def archive_record(record,success):
    id = record["ID"]
    source = record["SourceAddress"]
    destination = record["DestinationAddress"]
    cc = record["CC_Address"]
    bcc = record["BCC_Address"]
    subject = record["Subject"]
    body = record["Body"]
    attachment = record["Attachment"]
    table = 'mail."MailArchive"' if success else 'mail."FailedMail"'

    try:
        cur = conn.cursor()
        sql = 'DELETE FROM mail."MailQueue" WHERE "ID" = %s;'
        params = (id,)
        if debug == True:
            q = sql
            for val in params:
                q = q.replace('%s',f"'{str(val)}'",1)
            print(q)
        if testing:
            print("Test mode enabled. No changes made")
        else:
            cur.execute(sql,params)
        sql = f"INSERT INTO {table}\n"
        sql += f'("DestinationAddress","SourceAddress","CC_Address","BCC_Address","Subject","Body","Attachment","DateSent")\n'
        sql += 'VALUES (%s,%s,%s,%s,%s,%s,%s,NOW());'
        params = (destination,source,cc,bcc,subject,body,attachment)
        if debug == True:
            q = sql
            for val in params:
                q = q.replace('%s',f"'{str(val)}'",1)
            print(q)
        if testing:
            print("Test mode enabled. No changes made")
        else:
            cur.execute(sql,params)
        conn.commit()
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

def usage():
    print("Usage: acs_messenger.py [options] [arguments]")
    print("Options:")
    print("  -h, --help      Show this help message and exit")
    print("  -d, --debug     Show SQL queries")
    print("  -t, --testing   No database changes made")
    print("  -n, --nonotify  No sms or email sent - useful for testing")
    print("  -e, --email     Override email recipient - useful for testing")
    print("  -p, --phone     Override sms/text recipient - useful for testing")

if __name__ == '__main__' and running_process_check():
    main()