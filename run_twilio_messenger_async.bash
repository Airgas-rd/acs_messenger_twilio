#!/usr/bin/bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
script_name=$(basename "$0")
script_base="${script_name%.*}"

JOB_COUNT=""

# --- Parse named arguments ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-count)
      JOB_COUNT="$2"
      shift 2
      ;;
    --job-count=*)
      JOB_COUNT="${1#*=}"
      shift
      ;;
    -*|--*)
      echo "Unknown option: $1"
      exit 1
      ;;
    *)
      echo "Unexpected argument: $1"
      exit 1
      ;;
  esac
done

JOB_COUNT="${JOB_COUNT:-1}" # Default to 1 job if count not specified

# --- Validate job count ---
if ! [[ "$JOB_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --job-count must be a positive integer. Exiting"
  exit 1
fi

if [ -f "$log_file" ]; then
  tmp_log=$(mktemp "${log_dir}/tmp_log.XXXXXX")
  tail -n 1000 "$log_file" > "$tmp_log" && mv "$tmp_log" "$log_file" # Last 5000 lines max
fi

source "$HOME/.bash_profile"

for i in $(seq 1 "$JOB_COUNT"); do
  job_id=$(printf "%02d" "$i")

  (
    exec python3 /home/netadmin/scripts/acs_messenger_twilio/acs_messenger_async.py \
      --debug --mode=notification --loop --job-id="$job_id"
  ) &

  (
    exec python3 /home/netadmin/scripts/acs_messenger_twilio/acs_messenger_async.py \
      --debug --mode=report --loop --job-id="$job_id"
  ) &
done

disown -a

exit 0