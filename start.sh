#!/bin/bash
set -euo pipefail
cr_script=$(/usr/bin/readlink -f -- "$0")
cr_source_dir="${cr_script%/*}"
exec "$cr_source_dir/cadence/crescendo_launch" --standalone "$@"
