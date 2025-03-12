import os
import re
import psutil
import getopt
import sys
import psycopg2
import sendgrid
import pprint
import base64
import datetime
import time
import json
from psycopg2.extras import DictCursor
from twilio.rest import Client
from sendgrid.helpers.mail import *

my_twilio_phone_number = "+18333655808"

# Values set in /etc/environment
sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN")
pgpassword = os.environ.get("PGPASSWORD")
user_home = os.environ.get("HOME")

db_params_filepath = f"{user_home}/scripts/db_params.json"

sg = None
sms_client = None
conn = None
help = False
debug = False
testing = False
nonotify = False
email_override = None
phone_override = None
mode = None

def main():
    try:
        # Get options and arguments
        opts, args = getopt.getopt(sys.argv[1:],
                                   "hdtnm:e:p:",
                                   ["help", "debug", "testing","mode=","no-notify","email=","phone="])
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
                email_override = arg.strip()
            elif opt.strip() in ["-p","--phone"]:
                global phone_override
                phone_override = arg.strip()
            elif opt.strip() in ["-m", "--mode"]:
                global mode
                mode = arg.strip()

        if help is True:
            usage()
            return

        if (mode is not None
            and not re.match('^reports?$',mode)
            and not re.match('^notifications?$',mode)):
            print(f"Invalid mode value: {mode}")
            usage()
            return

        initialize() # set up db connection and clients
        while True:
            records = fetch_records()
            if len(records) < 1:
                time.sleep(1)
                continue
            for record in records:
                result = process_record(record)
                archive_record(record,result)
    except getopt.GetoptError as e:
        print(e)
        usage()
    except psycopg2.Error as e:
        print(f"Database Error: {e}")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()


def initialize():
    try:
        db_params = None
        global conn, sg, sms_client

        with open(db_params_filepath) as db_params_file:
            db_params = json.load(db_params_file)

        if debug is True: print(db_params)
        db_params["password"] = pgpassword
        conn = psycopg2.connect(**db_params, cursor_factory = DictCursor)
        sg = sendgrid.SendGridAPIClient(sendgrid_api_key) # Sendgrid email client
        sms_client = Client(account_sid, auth_token) # Twilio sms client
    except Exception as e:
        raise Exception(e)


def fetch_records():
    constraint = "TRUE" # Gets all records
    if mode is not None and re.match('^reports?$',mode):
        constraint = '"Attachment" IS NOT NULL' # Records WITH DATA in "Attachment" column
    elif mode is not None and re.match('^notifications?$',mode): #
        constraint = '"Attachment" IS NULL' # Records with NO data in "Attachment"

    sql = f"""
    SELECT
        "ID",
        "DestinationAddress",
        "SourceAddress",
        "CC_Address",
        "BCC_Address",
        "Subject",
        "Body",
        "Attachment",
        COALESCE(OCTET_LENGTH("Attachment"),0) AS "AttachmentLength",
        "attempts"
    FROM mail."MailQueue"
    WHERE "deliveryMethod" IS NULL
    AND {constraint}
    FOR UPDATE;
    """
    if debug is True:
        print(sql)
    rows = []
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if cur:
            cur.close()
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
        if phone_override is not None:
            record["DestinationAddress"] = phone_override
        destination = record["DestinationAddress"].strip().split('@')
        target_phone_number = destination[0]
        domain = destination[1] if len(destination) > 1 else None
        subject = record["Subject"].strip()
        body = record["Body"].strip()
        msg = None
        if domain is not None: # It's a device
            msg = f"SUBJ:{subject}\nMSG:{body}"
        else:
            msg = body

        if nonotify is True:
            print(f"Notifications disabled. No messages will be sent to {target_phone_number}")
            return True # pretend like it worked
        message = sms_client.messages.create(
            to = target_phone_number,  # Replace with the recipient"s phone number
            from_ = my_twilio_phone_number,  # Replace with your Twilio phone number
            body = msg,
        )
        if debug is True:
            print(f"Message to {target_phone_number}")
            print(f"Body: {msg}")
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
        sender = record["SourceAddress"]
        sender = 'bamsupport@airgas-rd.com' # override until mail.airgas-rd.com is validated with twilio
        mail = Mail(
            from_email = sender,
            subject = record["Subject"],
            html_content = record["Body"]
        )
        personalization = Personalization()
        personalization.add_to(To(recipient))
        if record["CC_Address"] is not None and len(record["CC_Address"].strip().split(',')) > 0:
            list = record["CC_Address"].strip().split(',')
            for cc in list:
                val = cc.strip()
                if len(val) < 6: continue # a@b.me -> anything shorter is prob bogus
                personalization.add_cc(Cc(val))
        if record["BCC_Address"] is not None and len(record["BCC_Address"].strip().split(',')) > 0:
            list = record["BCC_Address"].strip().split(',')
            for bcc in list:
                val = bcc.strip()
                if len(val) < 6: continue # a@b.me -> anything shorter is prob bogus
                personalization.add_bcc(Bcc(val))
        mail.add_personalization(personalization)
        if  record["AttachmentLength"] > 5: # Arbitrary threshold for the number of bytes until we can check for null value in Attachment
            basename = re.sub("\\s+","_",record["Subject"].strip().lower()) # acs_report_name
            suffix = datetime.datetime.now(datetime.timezone.utc).strftime("_%Y_%m_%d_%H_%M_%S.csv")
            name = basename + suffix # acs_report_name_YYYY_mm_dd_HH_MM_SS.csv
            file_name = FileName(name)
            file_content = FileContent(base64.b64encode(record["Attachment"]).decode('utf-8'))
            file_type = FileType("text/csv")
            disposition = Disposition("attachment")
            attachment = Attachment(file_content,file_name,file_type,disposition)
            mail.add_attachment(attachment)
            if nonotify is True:
                print(f"Notifications disabled. No messages will be sent to {email_override}")
                return True # pretend like it worked
        response = sg.client.mail.send.post(request_body = mail.get())
        if debug is True:
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
    table = 'mail."MailArchive"' if success else 'mail."FailedMail"'
    try:
        cur = conn.cursor()
        sql = 'DELETE FROM mail."MailQueue" WHERE "ID" = %s;'
        params = (id,)
        if debug is True:
            q = sql
            for val in params:
                q = q.replace('%s',f"'{str(val)}'",1)
            print(q)
        if testing:
            print("Test mode enabled. No changes made")
        else:
            cur.execute(sql,params)
        sql = f"INSERT INTO {table}\n"
        sql += f'("DestinationAddress","SourceAddress","CC_Address","BCC_Address","Subject","Body","DateSent")\n'
        sql += 'VALUES (%s,%s,%s,%s,%s,%s,NOW());'
        params = (destination,source,cc,bcc,subject,body) # discard attachments after sending
        if debug is True:
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
    finally:
        if cur:
            cur.close()


def running_process_check():
    pid = os.getpid()
    for process in psutil.process_iter(["pid","name"]):
        if process.info["name"] == __file__ and process.info["pid"] is not pid:
            print(f"{__file__} is already running.")
            return False
    return True

def usage():
    print("Usage: acs_messenger.py [options] [arguments]")
    print("Options:")
    print("  -m, --mode      report(s) | notification(s) DEFAULT (send both)")
    print("  -d, --debug     Show SQL queries")
    print("  -t, --testing   No database changes made")
    print("  -n, --nonotify  No sms or email sent - useful for testing")
    print("  -e, --email     Override email recipient - useful for testing")
    print("  -p, --phone     Override sms/text recipient - useful for testing")
    print("  -h, --help      Show this help message and exit")


if __name__ == '__main__' and running_process_check():
    main()
