#!/bin/sh

# Submission needs caller HOME for existing scheduler authentication. Restore
# only the owned simulation HOME here, after the scheduler has accepted the job.
case "${CRESCENDO_SIMULATION_HOME_SET-unrecorded}" in
  x) HOME=${CRESCENDO_SIMULATION_HOME_VALUE-}; export HOME ;;
  '') unset HOME ;;
  unrecorded) : ;;
  *) echo "Invalid Crescendo simulation HOME marker" >&2; exit 64 ;;
esac
unset CRESCENDO_SIMULATION_HOME_SET CRESCENDO_SIMULATION_HOME_VALUE

if [ "$#" -lt 1 ]; then
  echo "usage: submit_spectre_job.sh REAL_SPECTRE [spectre arguments ...]" >&2
  exit 64
fi

real_spectre=$1
shift
timeout_seconds=${CRESCENDO_SIMULATION_TIMEOUT_SECONDS:-900}
cadence_root=${CRESCENDO_CADENCE_ROOT:-}
timeout_log=${CRESCENDO_SPECTRE_TIMEOUT_LOG:-$PWD/cr_spectre_timeout.log}
retain_root=${CRESCENDO_SPECTRE_RETAIN_ROOT:-}
retain_pointer=${CRESCENDO_SPECTRE_RETAIN_POINTER:-}
spectre_log=
expect_log_path=0

for argument in "$@"; do
  if [ "$expect_log_path" -eq 1 ]; then
    spectre_log=$argument
    expect_log_path=0
    continue
  fi
  case "$argument" in
    +log|-log)
      expect_log_path=1
      ;;
    +log=*|-log=*)
      spectre_log=${argument#*=}
      ;;
  esac
done

if [ ! -x "$real_spectre" ]; then
  echo "Spectre executable is missing or not executable: $real_spectre" >&2
  exit 127
fi

case "${LD_PRELOAD:-}" in
  *"${cadence_root}"*"/libstdc++.so"*)
    if [ -n "$cadence_root" ]; then unset LD_PRELOAD; fi
    ;;
esac

finish_complete=0
simulation_pid=

finish_run() {
  finish_returncode=$1
  if [ "$finish_complete" -eq 1 ]; then
    return
  fi
  finish_complete=1
  end_epoch=$(date +%s.%N 2>/dev/null || date +%s)
  printf 'END epoch=%s pid=%s returncode=%s\n' "$end_epoch" "$$" "$finish_returncode" >> "$timeout_log"

  if [ -z "$retain_root" ] && [ -n "$retain_pointer" ] && [ -f "$retain_pointer" ] && [ ! -L "$retain_pointer" ]; then
    IFS= read -r retain_root < "$retain_pointer" || retain_root=
  fi

  case "$retain_root" in
    /*)
      if [ -n "$spectre_log" ] && [ -d "$retain_root" ] && [ ! -L "$retain_root" ]; then
        case "$spectre_log" in
          /*) source_log=$spectre_log ;;
          *) source_log=$PWD/$spectre_log ;;
        esac
        source_directory=${source_log%/*}
        source_basename=${source_log##*/}
        if canonical_directory=$(cd "$source_directory" 2>/dev/null && pwd -P); then
          source_log=$canonical_directory/$source_basename
        fi
        if [ -f "$source_log" ] && [ ! -L "$source_log" ]; then
          umask 077
          temporary_log=$(mktemp "$retain_root/.retained_spectre_XXXXXX" 2>/dev/null || true)
          if [ -n "$temporary_log" ] && cp "$source_log" "$temporary_log" 2>/dev/null && [ -s "$temporary_log" ]; then
            token=${temporary_log##*/.retained_spectre_}
            retained_log=$retain_root/retained_spectre_${token}.log
            if mv "$temporary_log" "$retained_log" 2>/dev/null; then
              temporary_meta=$(mktemp "$retain_root/.retained_spectre_meta_XXXXXX" 2>/dev/null || true)
              if [ -n "$temporary_meta" ]; then
                {
                  printf 'schema_version=crescendo.retained_spectre_log.v1\n'
                  printf 'source_path=%s\n' "$source_log"
                  printf 'source_cwd=%s\n' "$PWD"
                  printf 'returncode=%s\n' "$finish_returncode"
                  printf 'finished_epoch=%s\n' "$end_epoch"
                } > "$temporary_meta"
                mv "$temporary_meta" "$retain_root/retained_spectre_${token}.meta" 2>/dev/null || rm -f "$temporary_meta"
              fi
            else
              rm -f "$temporary_log"
            fi
          elif [ -n "$temporary_log" ]; then
            rm -f "$temporary_log"
          fi
        fi
      fi
      ;;
  esac
}

interrupt_run() {
  interrupt_returncode=$1
  interrupt_signal=$2
  if [ -n "$simulation_pid" ]; then
    kill -s "$interrupt_signal" "$simulation_pid" 2>/dev/null || true
  fi
  finish_run "$interrupt_returncode"
  trap - HUP INT TERM
  exit "$interrupt_returncode"
}

trap 'interrupt_run 129 HUP' HUP
trap 'interrupt_run 130 INT' INT
trap 'interrupt_run 143 TERM' TERM

start_epoch=$(date +%s.%N 2>/dev/null || date +%s)
printf 'START epoch=%s pid=%s cwd=%s argv=%s\n' "$start_epoch" "$$" "$PWD" "$*" >> "$timeout_log"

exec 3<&0
if [ "$timeout_seconds" -gt 0 ] 2>/dev/null && [ -x /usr/bin/timeout ]; then
  /usr/bin/timeout -k 15s "${timeout_seconds}s" "$real_spectre" "$@" <&3 3<&- &
else
  "$real_spectre" "$@" <&3 3<&- &
fi
simulation_pid=$!
wait "$simulation_pid"
returncode=$?
simulation_pid=
exec 3<&-
finish_run "$returncode"
trap - HUP INT TERM
exit "$returncode"
