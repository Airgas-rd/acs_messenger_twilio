import os
import re
import psutil
import sys
import json
import time
import signal
import random
import logging
import platform
import base64
import getopt
import pprint
import datetime
import psycopg2
import sendgrid

from psycopg2.extras import DictCursor
from twilio.rest import Client
from sendgrid.helpers.mail import *
from logging.handlers import TimedRotatingFileHandler

my_twilio_phone_number = "+18333655808"
twilio_magic_number_for_testing = "+15005550006"
hostname = platform.node().split('.')[0]

# Env vars set in /etc/environment
sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN")
pgpassword = os.environ.get("PGPASSWORD")
user_home = os.environ.get("HOME")

# DB config
with open(f"{user_home}/scripts/db_params.json") as f:
    db_params = json.load(f)
    db_params["password"] = pgpassword

# Globals
sg = None
sms_client = None
should_terminate = False

# CLI defaults
debug_mode = False
testing = False
no_notify = False
loop = False
mode = None
job_id = None
interval = 1.0
log_dir = None
email_override = None
phone_override = None
my_process_identifier = None

# Constants
FETCH_LIMIT = 5
MAX_ATTEMPTS = 3
MAX_AGE = 15
ROW_LOCK_NAMESPACE = 91784  # Arbitrary hardcoded namespace for row-level locks

def shutdown(signum, frame):
    global should_terminate
    logging.info(f"Received signal {signum}. Shutting down...")
    should_terminate = True

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def fetch_records():
    constraint = "TRUE"  # Gets all records
    if mode == 'report':
        constraint = '"Attachment" IS NOT NULL'
    elif mode == 'notification':
        constraint = '"Attachment" IS NULL'

    select_sql = f"""
    SELECT "ID", processed_by
    FROM mail."MailQueue"
    WHERE "deliveryMethod" IS NULL
      AND (
          processed_by IS NULL OR
          processed_by = %s OR
          (processed_by <> %s AND created_at < NOW() - '{MAX_AGE} minutes'::interval)
      )
      AND pg_try_advisory_xact_lock("ID")
      AND {constraint}
      AND attempts <= {MAX_ATTEMPTS}
    ORDER BY "ID" ASC
    LIMIT {FETCH_LIMIT}
    FOR UPDATE SKIP LOCKED
    """

    claimed_rows = []
    try:
        with psycopg2.connect(**db_params, cursor_factory=DictCursor) as conn:
            with conn.cursor() as cur:
                if debug_mode:
                    logging.debug(cur.mogrify(select_sql, (my_process_identifier, my_process_identifier)).decode())

                cur.execute(select_sql, (my_process_identifier, my_process_identifier))
                rows = cur.fetchall()

                lock_query = "SELECT pg_try_advisory_xact_lock(%s);"
                for row in rows:
                    if debug_mode:
                        logging.debug(cur.mogrify(lock_query, (row["ID"],)).decode())
                    cur.execute(lock_query, (row["ID"],))
                    if not cur.fetchone()[0]: # lock not acquired for the row
                        if debug_mode:
                            logging.debug(f'Could not acquire lock for record id {row["ID"]}')
                        continue

                    update_filter = None
                    if row["processed_by"] is None:
                        update_filter = "processed_by IS NULL"
                    else:
                        update_filter = f"""processed_by = '{row["processed_by"]}'"""

                    update_sql = f"""
                    UPDATE mail."MailQueue"
                    SET processed_by = %s, attempts = attempts + 1
                    WHERE "ID" = %s
                    AND {update_filter} -- The processed_by ensures the row is still in the same state from the select
                    RETURNING "ID", "DestinationAddress", "SourceAddress", "CC_Address", "BCC_Address", "Subject", "Body", "Attachment", processed_by
                    """

                    if debug_mode:
                        logging.debug(cur.mogrify(update_sql, (my_process_identifier, row["ID"])).decode())

                    try:
                        cur.execute(update_sql, (my_process_identifier, row["ID"]))
                        record = cur.fetchone()
                        if record:
                            claimed_rows.append(record)
                    except Exception as e:
                        logging.error(f'Error setting processed_by value ({my_process_identifier}) for record id ({row["ID"]})')


                if testing:
                    conn.rollback()
                else:
                    conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Error retrieving messages: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")

    return claimed_rows

def process_record(record):
    destination = record["DestinationAddress"]
    target = destination.strip().split('@')[0]
    if len(destination) < 6 or len(target) < 1: # a@b.me -> anything shorter is prob bogus
        logging.error(f"Invalid destination address: {destination}")
        return False
    result = None
    if re.fullmatch(r"\d{10}", target): # phone number
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
            logging.debug(f"Notifications disabled. No messages will be sent to {target_phone_number}")
            return True # pretend like it worked
        message = sms_client.messages.create(
            to = target_phone_number,  # Replace with the recipient"s phone number
            from_ = my_twilio_phone_number,  # Replace with your Twilio phone number
            body = msg,
        )
        if debug_mode:
            logging.debug(f"Message to {target_phone_number}")
            logging.debug(f"Body: {msg}")
            logging.debug(f"Status: {message.status}")
        if message.error_code:
            raise Exception(f"SMS error {message.error_code} {message.error_message}")
    except Exception as e:
        logging.exception(f"Error in send_sms: {e}")
        return False
    return True

def send_email(record):
    try:
        if email_override:
            record["DestinationAddress"] = email_override
        recipient = record["DestinationAddress"]
        if not re.fullmatch(r"[^@]+@[^@]+\.[^@]+", recipient):
            logging.error(f"Malformed email address: {recipient}")
            return False
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
            cc_list = record["CC_Address"].strip().split(',')
            for cc in cc_list:
                val = cc.strip()
                if not re.fullmatch(r"[^@]+@[^@]+\.[^@]+",val):
                    logging.error(f"Ignoring malformed CC recipient ({val})")
                    continue
                personalization.add_cc(Cc(val))
        if record["BCC_Address"] is not None and len(record["BCC_Address"].strip().split(',')) > 0:
            bcc_list = record["BCC_Address"].strip().split(',')
            for bcc in bcc_list:
                val = bcc.strip()
                if not re.fullmatch(r"[^@]+@[^@]+\.[^@]+",val):
                    logging.error(f"Ignoring malformed BCC recipient ({val})")
                    continue
                personalization.add_bcc(Bcc(val))
        mail.add_personalization(personalization)
        if record["Attachment"] and len(record["Attachment"]) > 0:
            basename = re.sub(r'[^\w\-_.]', '_', record["Subject"].strip().lower()) # acs_report_name
            suffix = datetime.datetime.now(datetime.timezone.utc).strftime("_%Y_%m_%d_%H_%M_%S.csv")
            name = basename + suffix # acs_report_name_YYYY_mm_dd_HH_MM_SS.csv
            file_name = FileName(name)
            file_content = FileContent(base64.b64encode(record["Attachment"]).decode('utf-8'))
            file_type = FileType("text/csv")
            disposition = Disposition("attachment")
            attachment = Attachment(file_content,file_name,file_type,disposition)
            mail.add_attachment(attachment)
        if no_notify is True:
            logging.debug(f"Notifications disabled. No messages will be sent to {recipient}")
            return True # pretend like it worked

        response = sg.client.mail.send.post(request_body = mail.get())

        if debug_mode:
            logging.debug("Email Payload")
            pprint.pprint(mail.get(), indent=4)
            logging.debug(f"Email response code: {response.status_code}")
        if response.status_code < 200 or response.status_code > 204:
            logging.debug(response.to_dict)
            raise Exception(f"Email request failed with code {response.status_code}")
    except Exception as e:
        logging.exception(f"Error in send_email: {e}")
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
    cur = None
    try:
        with psycopg2.connect(**db_params, cursor_factory = DictCursor) as conn:
            with conn.cursor() as cur:
                delete_sql = 'DELETE FROM mail."MailQueue" WHERE "ID" = %s;'
                params = (id,)

                if debug_mode:
                    logging.debug(cur.mogrify(delete_sql,params).decode())

                if testing:
                    logging.debug("Test mode enabled. No changes made")
                else:
                    cur.execute(delete_sql,params)

                insert_sql = f"INSERT INTO {table}\n"
                insert_sql += f'("DestinationAddress","SourceAddress","CC_Address","BCC_Address","Subject","Body",processed_by,"DateSent")\n'
                insert_sql += 'VALUES (%s,%s,%s,%s,%s,%s,%s,NOW());'
                params = (destination,source,cc,bcc,subject,body,processed_by) # discard attachments after sending

                if debug_mode:
                    logging.debug(cur.mogrify(insert_sql,params).decode())

                if testing:
                    logging.debug("Test mode enabled. No changes made")
                else:
                    cur.execute(insert_sql,params)
                    conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Error in archive_record: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in archive_record: {e}")

def running_process_check():
    global my_process_identifier
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
                            other_mode = parameter.split('=')[-1].rstrip('s')
                        else: # short format -k 'v'
                            other_mode = args[i].rstrip('s')

                    elif parameter in ["-j","--job-id"] or re.match('--job-id=',parameter): # found job id
                        if re.match('--job-id=',parameter):
                            other_job_id = parameter.split('=')[-1]
                        else:
                             other_job_id = args[i]
                    if my_process_identifier == f"{hostname}-{other_mode}-{other_job_id}":
                        if debug_mode:
                            logging.debug(f'{script} with identifier "{my_process_identifier}" found. Exiting')
                        return False
                    i += 1
    return True

def print_usage():
    print("""
Usage: acs_messenger.py [options] [arguments]
Options:
  -m, --mode        report | notification (default: all)
  -l, --loop        Run continuously (polls DB every second)
  -d, --debug       Enable debug output
  -t, --testing     Dry run (no DB changes)
  -n, --no-notify   Skip sending SMS or email
  -e, --email       Override email recipient
  -p, --phone       Override SMS recipient
  -j, --job-id      Custom job identifier
  -i, --interval    Polling interval (seconds)
  -L, --log-dir     Custom log directory
  -h, --help        Show this help message and exit
""")

def parse_args():
    global mode, loop, debug_mode, testing, no_notify, email_override
    global phone_override, job_id, interval, log_dir, my_process_identifier

    try:
        opts, _ = getopt.getopt(sys.argv[1:], "hdtnlm:e:p:j:i:L:", [
            "help", "debug", "testing", "mode=", "no-notify", "loop",
            "email=", "phone=", "job-id=", "interval=", "log-dir="
        ])
        for opt, arg in opts:
            if opt in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif opt in ["-d", "--debug"]:
                debug_mode = True
            elif opt in ["-t", "--testing"]:
                testing = True
            elif opt in ["-n", "--no-notify"]:
                no_notify = True
            elif opt in ["-l", "--loop"]:
                loop = True
            elif opt in ["-e", "--email"]:
                email_override = arg.strip()
            elif opt in ["-p", "--phone"]:
                phone_override = twilio_magic_number_for_testing if arg.strip().lower() == 'twilio' else arg.strip()
            elif opt in ["-m", "--mode"]:
                mode = re.sub(r's$', '', arg.strip().lower())
            elif opt in ["-j", "--job-id"]:
                job_id = arg.strip()
            elif opt in ["-i", "--interval"]:
                interval = float(arg.strip())
            elif opt in ["-L", "--log-dir"]:
                log_dir = os.path.abspath(arg.strip())
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        print_usage()
        sys.exit(1)

    if mode not in (None, 'report', 'notification'):
        logging.error(f"Invalid mode value: {mode}")
        print_usage()
        sys.exit(1)

    my_process_identifier = hostname
    if mode:
        my_process_identifier += f"-{mode}"
    if job_id:
        my_process_identifier += f"-{job_id}"
    my_process_identifier = my_process_identifier.lower()

def initialize_logs():
    global log_dir,my_process_identifier
    try:
        if log_dir is None:
            current_file_path = __file__ # Special variable holding the path of the current file
            parent_dir_current = os.path.dirname(current_file_path)
            log_dir = os.path.join(parent_dir_current,"logs")
        os.makedirs(log_dir, exist_ok=True)
        file_name = f"{my_process_identifier}.log"
        log_file = os.path.join(log_dir, file_name)

        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            interval=1,
            backupCount=7
        )

        handlers = [file_handler]

        if debug_mode:
            stream_handler = logging.StreamHandler(sys.stdout)
            handlers.append(stream_handler)

        logging.basicConfig(
            level=logging.DEBUG if debug_mode else logging.INFO,
            format='%(asctime)s %(levelname)s [%(process)d] %(message)s',
            handlers=handlers
        )
    except Exception as e:
        print(f"Failed to initialize logging: {e}", file=sys.stderr)
        sys.exit(1)

def initialize_clients():
    global sg, sms_client
    sg = sendgrid.SendGridAPIClient(sendgrid_api_key)
    sms_client = Client(account_sid, auth_token)

def run_worker_loop():
    while not should_terminate:
        records = fetch_records()
        processed_count = 0
        failed_count = 0
        for record in records:
            if debug_mode:
                logging.debug(f"Processing record ID: {record['ID']}")
            success = process_record(record)
            archive_record(record, success)
            if success:
                processed_count += 1
            if not success:
                failed_count += 1
        if debug_mode and records:
            logging.debug(f"Batch complete. Processed: {processed_count}, Failed: {failed_count}")
        if not loop:
            break
        time.sleep(interval * random.uniform(0.8, 1.2)) # Don't hammer the DB

def main():
    parse_args()
    if running_process_check():
        initialize_logs()
        initialize_clients()
        run_worker_loop()

if __name__ == '__main__':
    main()
