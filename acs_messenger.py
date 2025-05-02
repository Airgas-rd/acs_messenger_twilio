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
import platform
import random
from psycopg2.extras import DictCursor
from twilio.rest import Client
from sendgrid.helpers.mail import *

my_twilio_phone_number = "+18333655808"
twilio_magic_number_for_testing = "+15005550006" # https://www.twilio.com/docs/messaging/tutorials/automate-testing
hostname = platform.node().split('.')[0]

# Values set in /etc/environment
sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN")
pgpassword = os.environ.get("PGPASSWORD")
user_home = os.environ.get("HOME")

db_params_filepath = f"{user_home}/scripts/db_params.json"

with open(db_params_filepath) as db_params_file:
     db_params = json.load(db_params_file)
     db_params["password"] = pgpassword

sg = None
sms_client = None
conn = None
help = False
debug = False
testing = False
no_notify = False
email_override = None
phone_override = None
mode = None
loop = False
job_id = None

my_process_identifier = None # Used to differentiate independent job processes - created in parse_args()

def main():
    try:
        initialize() # set up db connection and clients
        while True:
            records = fetch_records()
            for record in records:
                result = process_record(record)
                archive_record(record,result)
            if loop is True:
                time.sleep(random.uniform(0.5,1.5))
                continue
            else:
                break
    except getopt.GetoptError as e:
        print(e)
        usage()
    except psycopg2.Error as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()


def initialize():
    try:
       
        global conn, sg, sms_client
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

    update = f"""
    UPDATE mail."MailQueue"
    SET processed_by = %s, attempts = attempts + 1
    WHERE "ID" IN (
        SELECT "ID"
        FROM mail."MailQueue"
        WHERE "deliveryMethod" IS NULL -- filter out CAM messages bc they fetch via REST
        AND (
            processed_by IS NULL -- record is available for processing
            OR processed_by = %s -- previously attempted but failed for some reason
            OR (processed_by <> %s AND created_at < NOW() - '15 minutes'::interval) -- previously attempted by another process that died or is falling behind
        )
        AND {constraint}
        AND attempts <= 3
        AND pg_try_advisory_xact_lock("ID") -- Acquire a transaction-level advisory lock
        ORDER BY "ID" ASC -- FIFO
        LIMIT 5
        FOR UPDATE SKIP LOCKED
    ) RETURNING "ID","DestinationAddress","SourceAddress","CC_Address","BCC_Address","Subject","Body","Attachment",processed_by
    """

    params = (my_process_identifier,my_process_identifier,my_process_identifier)

    if debug is True:
        q = update
        for val in params:
            q = q.replace('%s',f"'{str(val)}'",1)
        print(q)

    # Acquire a session level advisory lock before the update
    rows = []
    try:
        with psycopg2.connect(**db_params, cursor_factory = DictCursor) as con:
            with con.cursor() as cur:
                lock_query = "SELECT pg_try_advisory_lock(%s);"
                lock_id = 4906  # unique lock ID
                
                if debug:
                    print(f"Acquiring advisory lock on lock id ({lock_id})")

                cur.execute(lock_query, (lock_id,))
                lock_acquired = cur.fetchone()[0]
                
                if not lock_acquired and not testing:
                    if debug:
                        print(f"Could not acquire advisory lock for process {my_process_identifier}. Trying again.")
                    return rows  # Skip processing if the lock cannot be acquired

                cur.execute(update,params)
                rows = cur.fetchall()
                if testing:
                    conn.rollback()

                unlock_query = "SELECT pg_advisory_unlock(%s);"
                if debug:
                    print(f"Releasing advisory lock on lock id ({lock_id})")
                cur.execute(unlock_query, (lock_id,))
    except psycopg2.Error as e:
        print(f"Error retrieving messages: {e}")
    finally:
        if con:
            con.close()
    
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
        if domain == 'txt.att.net': # It's a device
            msg = f"SUBJ:{subject}\nMSG:{body}"
        else:
            msg = body

        if no_notify is True:
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
        print("Error in send_sms: {e}")
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
            plain_text_content = record["Body"]
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
        if  record["Attachment"] is not None: # Arbitrary threshold for the number of bytes until we can check for null value in Attachment
            basename = re.sub("\\s+","_",record["Subject"].strip().lower()) # acs_report_name
            suffix = datetime.datetime.now(datetime.timezone.utc).strftime("_%Y_%m_%d_%H_%M_%S.csv")
            name = basename + suffix # acs_report_name_YYYY_mm_dd_HH_MM_SS.csv
            file_name = FileName(name)
            file_content = FileContent(base64.b64encode(record["Attachment"]).decode('utf-8'))
            file_type = FileType("text/csv")
            disposition = Disposition("attachment")
            attachment = Attachment(file_content,file_name,file_type,disposition)
            mail.add_attachment(attachment)
        if no_notify is True:
                print(f"Notifications disabled. No messages will be sent to {recipient}")
                return True # pretend like it worked
        
        response = sg.client.mail.send.post(request_body = mail.get())
        
        if debug is True:
            print("Email Payload")
            pprint.pprint(mail.get(),indent=4)
            print(f"Email response code: {response.status_code}")
        if response.status_code < 200 or response.status_code > 204:
            print(response.to_dict)
            raise Exception(f"Email request failed with code {response.status_code}")
    except Exception as e:
        print(f"Error in send_email: {e}")
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
    processed_by = record["processed_by"]
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
        sql += f'("DestinationAddress","SourceAddress","CC_Address","BCC_Address","Subject","Body",processed_by,"DateSent")\n'
        sql += 'VALUES (%s,%s,%s,%s,%s,%s,%s,NOW());'
        params = (destination,source,cc,bcc,subject,body,processed_by) # discard attachments after sending
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
        print(f"Error in archive_record: {e}")
    finally:
        if cur:
            cur.close()


def running_process_check():
    mypid = os.getpid()
    myscriptname = os.path.basename(__file__)
    
    for process in psutil.process_iter(["cmdline","pid"]):
        pid = process.info["pid"]
        cmdline = process.info["cmdline"]
        if cmdline is None:
            continue
        for idx, val in enumerate(cmdline):
            script = os.path.basename(val)
            if script == myscriptname and pid != mypid:
                args = cmdline[idx+1:]
                other_mode = ""
                other_job_id = ""
                i = 0
                while i < len(args):
                    parameter = args[i]
                    if parameter in ["-m","--mode"] or re.match('--mode=',parameter): # found mode
                        if re.match('--mode=',parameter): # long format --key=value
                            other_mode = parameter.split('=')[-1]
                        else: # short format -k 'v'
                            other_mode = args[i]

                        if re.match('^reports?$',other_mode):
                            other_mode = 'report'
                        elif re.match('^notifications?$',other_mode):
                            other_mode = 'notification' 
                    elif parameter in ["-j","--job-id"] or re.match('--job-id=',parameter): # found job id
                        if re.match('--job-id=',parameter):
                            other_job_id = parameter.split('=')[-1]
                        else:
                             other_job_id = args[i]
                    if my_process_identifier == f"{hostname}-{other_mode}-{other_job_id}":
                        if debug:
                            print(f'{script} with identifier "{my_process_identifier}" found. Exiting')
                        return False   
                    i += 1             
    return True


def usage():
    print("Usage: acs_messenger.py [options] [arguments]")
    print("Options:")
    print("  -m, --mode      report | notification DEFAULT (send both)")
    print("  -l, --loop      Script run perpetually - polls DB every second for new records")
    print("  -d, --debug     Show SQL queries")
    print("  -t, --testing   No database changes made")
    print("  -n, --no-notify No sms or email sent - useful for testing")
    print("  -e, --email     Override email recipient - useful for testing")
    print("  -p, --phone     Override sms/text recipient - useful for testing")
    print("  -j, --job-id    User defined id for the current job. Ex. 01")
    print("  -h, --help      Show this help message and exit")


def parse_args():
    # Get options and arguments
    opts, args = getopt.getopt(sys.argv[1:],"hdtnlm:e:p:j:",
            ["help", "debug", "testing","mode=","no-notify","loop","email=","phone=","job-id="])
    for opt, arg in opts:
        if opt in ["-h","--help"]:
            global help
            help = True
        elif opt in ["-d","--debug"]:
            global debug
            debug = True
        elif opt in ["-t","--testing"]:
            global testing, my_twilio_phone_number
            testing = True
            my_twilio_phone_number = twilio_magic_number_for_testing
        elif opt in ["-n","--no-notify"]:
            global no_notify
            no_notify = True
        elif opt in ["-l","--loop"]:
            global loop
            loop = True
        elif opt.strip() in ["-e","--email"]:
            global email_override
            email_override = arg.strip()
        elif opt.strip() in ["-p","--phone"]:
            global phone_override
            phone_override = arg.strip()
        elif opt.strip() in ["-m", "--mode"]:
            global mode
            if re.match('^reports?$',arg.strip().lower()):
                mode = 'report'
            if re.match('^notifications?$',arg.strip().lower()):
                mode = 'notification'
        elif opt.strip() in ["-j", "--job-id"]:
            global job_id
            job_id = arg.strip()

    if help is True:
        usage()
        sys.exit(0)

    if mode is not None and mode != 'report' and mode != 'notification':
        print(f"Invalid mode value: {mode}")
        usage()
        sys.exit(1)
    
    global my_process_identifier # ex. server1-report-01
    my_process_identifier = hostname
    if mode is not None:
        my_process_identifier += f"-{mode}"
    if job_id is not None:
        my_process_identifier += f"-{job_id}"



if __name__ == '__main__':
    parse_args()
    if running_process_check():
        main()
