#!/usr/bin/env bash

directory="/home/netadmin/scripts/acs_messenger_twilio"
filename="acs_messenger_async.py"
script="$directory/$filename"
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

JOB_COUNT="${JOB_COUNT:-1}" # Default to 1 job

# --- Validate job count ---
if ! [[ "$JOB_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --job-count must be a positive integer. Exiting"
  exit 1
fi

# Source environment if available
[[ -f "$HOME/.bash_profile" ]] && source "$HOME/.bash_profile"

function launch_jobs() {
    local mode=$1

    proc_count=$(ps -ef | grep "[p]ython3.*${filename}.*${mode}" | wc -l)
    if [[ $proc_count -lt $JOB_COUNT ]]; then
        start_id=$((proc_count + 1))
        for i in $(seq $start_id $JOB_COUNT); do
          job_id=$(printf "%02d" "$i")
          echo "Launching $mode job $job_id"
          nohup python3 "$script" --mode="$mode" --job-id="$job_id" --loop --debug &
        done
    fi
}

launch_jobs "notification" &
launch_jobs "report" &

# Disown all backgrounded jobs
disown -a
exit 0
