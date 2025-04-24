import asyncio
import os
import re
import psutil
import getopt
import sys
import asyncpg
import pprint
import base64
import datetime
import json
import platform
from sendgrid.helpers.mail import *

my_twilio_phone_number = "+18333655808"
hostname = platform.node().split('.')[0]

# Values set in /etc/environment
sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
auth_token = os.environ.get("TWILIO_CLIENT_AUTH_TOKEN")
pgpassword = os.environ.get("PGPASSWORD")
user_home = os.environ.get("HOME")

db_params_filepath = f"{user_home}/scripts/db_params.json"

sg = None
sms_client = None
db_pool = None  # Global connection pool
help = False
debug = False
testing = False
nonotify = False
email_override = None
phone_override = None
mode = None
loop = False

# Semaphore to control the number of concurrent async calls
MAX_CONCURRENT_TASKS = psutil.cpu_count(logical=True)
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

MAX_DB_CONNECTIONS = MAX_CONCURRENT_TASKS / 3 # 3 to 1 task to connection ratio

async def main():
    try:
        await initialize()
        while True:
            records = await fetch_records()  # Fetch records asynchronously
            tasks = [process_record(record) for record in records]
            results = await asyncio.gather(*tasks)
            for record, result in zip(records, results):
                await archive_record(record, result)  # Archive records asynchronously
            if db_pool:
                await db_pool.close()  # Close the connection pool
            if loop is True:
                await asyncio.sleep(1)
                continue
            else:
                break
    except getopt.GetoptError as e:
        print(e)
        usage()
    except asyncpg.PostgresError as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if db_pool:
            await db_pool.close()  # Close the connection pool

async def initialize():
    """Initialize the asyncpg connection pool."""
    global db_pool
    try:
        db_params = None
        with open(db_params_filepath) as db_params_file:
            db_params = json.load(db_params_file)
        db_params["password"] = pgpassword

        # Create a connection pool
        db_pool = await asyncpg.create_pool(
            host=db_params["host"],
            port=db_params["port"],
            user=db_params["user"],
            password=db_params["password"],
            database=db_params["dbname"],
            min_size=1,  # Minimum number of connections
            max_size=MAX_CONCURRENT_TASKS  # Maximum number of connections
        )
        print("Database connection pool initialized.")
    except Exception as e:
        raise Exception(f"Error initializing database connection pool: {e}")


async def process_record(record):
    async with semaphore:  # Limit the number of concurrent tasks
        destination = record["DestinationAddress"]
        target = destination.strip().split('@')[0]
        if len(destination) < 6 or len(target) < 1:  # a@b.me -> anything shorter is prob bogus
            print(f"Invalid destination address: {destination}")
            return False
        result = None
        if re.search("^\\d{10}", target):  # assume phone number
            result = await send_sms(record)
        else:  # assume email
            result = await send_email(record)
        return result


async def send_sms(record):
    try:
        if phone_override is not None:
            record["DestinationAddress"] = phone_override
        destination = record["DestinationAddress"].strip().split('@')
        target_phone_number = destination[0]
        domain = destination[1] if len(destination) > 1 else None
        subject = record["Subject"].strip()
        body = record["Body"].strip()
        msg = None
        if domain is not None:  # It's a device
            msg = f"SUBJ:{subject}\nMSG:{body}"
        else:
            msg = body

        if nonotify is True:
            print(f"Notifications disabled. No messages will be sent to {target_phone_number}")
            return True  # pretend like it worked
        message = await asyncio.to_thread(
            sms_client.messages.create,
            to=target_phone_number,
            from_=my_twilio_phone_number,
            body=msg,
        )
        if debug is True:
            print(f"Message to {target_phone_number}")
            print(f"Body: {msg}")
            print(f"Status: {message.status}")
        if message.error_code:
            raise Exception(f"SMS error {message.error_code} {message.error_message}")
    except Exception as e:
        print(f"Error in send_sms: {e}")
        return False
    return True


async def send_email(record):
    try:
        if email_override:
            record["DestinationAddress"] = email_override
        recipient = record["DestinationAddress"]
        sender = record["SourceAddress"]
        sender = 'bamsupport@airgas-rd.com'  # override until mail.airgas-rd.com is validated with twilio
        mail = Mail(
            from_email=sender,
            subject=record["Subject"],
            plain_text_content=record["Body"]
        )
        personalization = Personalization()
        personalization.add_to(To(recipient))
        if record["CC_Address"] is not None and len(record["CC_Address"].strip().split(',')) > 0:
            list = record["CC_Address"].strip().split(',')
            for cc in list:
                val = cc.strip()
                if len(val) < 6:
                    continue  # a@b.me -> anything shorter is prob bogus
                personalization.add_cc(Cc(val))
        if record["BCC_Address"] is not None and len(record["BCC_Address"].strip().split(',')) > 0:
            list = record["BCC_Address"].strip().split(',')
            for bcc in list:
                val = bcc.strip()
                if len(val) < 6:
                    continue  # a@b.me -> anything shorter is prob bogus
                personalization.add_bcc(Bcc(val))
        mail.add_personalization(personalization)
        if record["Attachment"] is not None:  # Arbitrary threshold for the number of bytes until we can check for null value in Attachment
            basename = re.sub("\\s+", "_", record["Subject"].strip().lower())  # acs_report_name
            suffix = datetime.datetime.now(datetime.timezone.utc).strftime("_%Y_%m_%d_%H_%M_%S.csv")
            name = basename + suffix  # acs_report_name_YYYY_mm_dd_HH_MM_SS.csv
            file_name = FileName(name)
            file_content = FileContent(base64.b64encode(record["Attachment"]).decode('utf-8'))
            file_type = FileType("text/csv")
            disposition = Disposition("attachment")
            attachment = Attachment(file_content, file_name, file_type, disposition)
            mail.add_attachment(attachment)
        if nonotify is True:
            print(f"Notifications disabled. No messages will be sent to {recipient}")
            return True  # pretend like it worked

        response = await asyncio.to_thread(sg.client.mail.send.post, request_body=mail.get())

        if debug is True:
            print("Email Payload")
            pprint.pprint(mail.get(), indent=4)
            print(f"Email response code: {response.status_code}")
        if response.status_code < 200 or response.status_code > 204:
            print(response.to_dict)
            raise Exception(f"Email request failed with code {response.status_code}")
    except Exception as e:
        print(f"Error in send_email: {e}")
        return False
    return True


async def fetch_records():
    """Fetch records from the database."""
    async with db_pool.acquire() as conn:  # Acquire a connection from the pool
        constraint = "TRUE"  # Gets all records
        if mode is not None and re.match('^reports?$', mode):
            constraint = '"Attachment" IS NOT NULL'  # Records WITH DATA in "Attachment" column
        elif mode is not None and re.match('^notifications?$', mode):
            constraint = '"Attachment" IS NULL'  # Records with NO data in "Attachment"

        sql = f"""
        UPDATE mail."MailQueue"
        SET processed_by = $1, attempts = attempts + 1
        WHERE "ID" IN (
            SELECT "ID" 
            FROM mail."MailQueue"
            WHERE "deliveryMethod" IS NULL
            AND (
                processed_by IS NULL
                OR processed_by = $1
                OR (processed_by <> $1 AND created_at < NOW() - INTERVAL '15 minutes')
            )
            AND {constraint}
            AND attempts <= 3
            ORDER BY "ID" ASC
            LIMIT {MAX_CONCURRENT_TASKS}
            FOR UPDATE SKIP LOCKED
        )
        RETURNING "ID", "DestinationAddress", "SourceAddress", "CC_Address", "BCC_Address", "Subject", "Body", "Attachment", processed_by;
        """
        params = (hostname,)

        if debug:
            q = sql
            q = q.replace('$1',f"'{str(hostname)}'")
            print(q)

        try:
            rows = await conn.fetch(sql, *params)  # Fetch all rows
            if not testing:
                conn.co
            return rows
        except asyncpg.PostgresError as e:
            print(f"Error in fetch_records: {e}")
            return []


async def archive_record(record, success):
    """Archive a record in the database."""
    async with db_pool.acquire() as conn:  # Acquire a connection from the pool
        id = record["ID"]
        source = record["SourceAddress"]
        destination = record["DestinationAddress"]
        cc = record["CC_Address"]
        bcc = record["BCC_Address"]
        subject = record["Subject"]
        body = record["Body"]
        table = 'mail."MailArchive"' if success else 'mail."FailedMail"'

        try:
            async with conn.transaction():  # Use a transaction
                # Delete the record from the queue
                sql_delete = 'DELETE FROM mail."MailQueue" WHERE "ID" = $1;'
                if debug:
                    q = sql_delete
                    q = q.replace('$1',id)
                    print(f"Executing SQL: {q}")
                if not testing:
                    await conn.execute(sql_delete, id)
                else:
                    print("Testing mode enabled: no changes made.")

                # Insert the record into the archive or failed table
                sql_insert = f"""
                INSERT INTO {table} ("DestinationAddress", "SourceAddress", "CC_Address", "BCC_Address", "Subject", "Body", "DateSent")
                VALUES ($1, $2, $3, $4, $5, $6, NOW());
                """
                params = (destination, source, cc, bcc, subject, body)
                if debug:
                    print(f"Executing SQL: {sql_insert} with params: {params}")
                if not testing:
                    await conn.execute(sql_insert, *params)
        except asyncpg.PostgresError as e:
            print(f"Error in archive_record: {e}")


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
                mode_args = [] 
                for arg in args:
                    if re.match('^.*reports?$',arg.strip()):
                        mode_args.append('reports')  # make them plural for easy string comp below
                    if re.match('^.*notifications?$',arg.strip()):
                        mode_args.append('notifications')
                if mode_args == []:
                    if debug:
                        print(f"{script} is already running in an undifferentiated mode - Sending reports AND notifications.")
                    return False
                for arg in mode_args:
                    if mode is None:
                        if debug:
                            print(f"{script} is already running in {arg} mode - Cannot run this instance in undifferentiated mode.")
                            return False
                    if mode in arg:
                        if debug:
                            print(f"{script} is already running in {mode} mode.")
                        return False
    return True


def usage():
    print("Usage: acs_messenger.py [options] [arguments]")
    print("Options:")
    print("  -m, --mode      report(s) | notification(s) DEFAULT (send both)")
    print("  -l, --loop      Script run perpetually - polls DB every second for new records")
    print("  -d, --debug     Show SQL queries")
    print("  -t, --testing   No database changes made")
    print("  -n, --no-notify No sms or email sent - useful for testing")
    print("  -e, --email     Override email recipient - useful for testing")
    print("  -p, --phone     Override sms/text recipient - useful for testing")
    print("  -h, --help      Show this help message and exit")


def parse_args():
    # Get options and arguments
    opts, args = getopt.getopt(sys.argv[1:],"hdtnlm:e:p:",
            ["help", "debug", "testing","mode=","no-notify","loop","email=","phone="])
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
            mode = arg.strip()

    if help is True:
        usage()
        sys.exit(0)

    if (mode is not None
        and not re.match('^reports?$',mode)
        and not re.match('^notifications?$',mode)):
        print(f"Invalid mode value: {mode}")
        usage()
        sys.exit(1)


if __name__ == '__main__':
    parse_args()
    if running_process_check():
        asyncio.run(main())

