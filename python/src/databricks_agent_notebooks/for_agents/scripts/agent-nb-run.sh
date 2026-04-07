#!/usr/bin/env bash
# agent-nb-run.sh — execution helper for agent-notebook run
#
# Handles path computation, output directory creation, early validation,
# and tee-to-log so agents can remember one script path instead of
# reconstructing commands from documentation.
#
# Usage:
#   agent-nb-run.sh <notebook> --profile <profile> [--output-dir <dir>] [...]
#
# All arguments are forwarded to `agent-notebook run`. The script only
# parses what it needs for its own logic (notebook path, --profile,
# --output-dir, --format); everything else passes through unchanged.
#
# The agent wraps this script in their environment's non-blocking pattern:
#   Claude Code:    run_in_background: true
#   nohup:          nohup agent-nb-run.sh ... > /dev/null 2>&1 &
#   Codex:          start in PTY session (tty: true)

set -euo pipefail

die() { echo "agent-nb-run: error: $1" >&2; exit 1; }

# --- Parse arguments we need; collect all args for forwarding ---

NOTEBOOK=""
PROFILE_FOUND=false
OUTPUT_DIR=""
FORMAT="md"

# We need the positional notebook arg and a few flags for our own logic.
# Everything gets forwarded to agent-notebook run via "$@".
args=("$@")
i=0
while [ $i -lt ${#args[@]} ]; do
    arg="${args[$i]}"
    # Handle --flag=value syntax: split into flag and value
    val=""
    case "$arg" in
        --*=*)
            val="${arg#*=}"
            arg="${arg%%=*}"
            ;;
    esac
    case "$arg" in
        --profile)
            PROFILE_FOUND=true
            if [ -z "$val" ]; then i=$((i + 1)); fi
            ;;
        --output-dir)
            if [ -n "$val" ]; then
                OUTPUT_DIR="$val"
            else
                i=$((i + 1))
                if [ $i -lt ${#args[@]} ]; then
                    OUTPUT_DIR="${args[$i]}"
                fi
            fi
            ;;
        --format)
            if [ -n "$val" ]; then
                FORMAT="$val"
            else
                i=$((i + 1))
                if [ $i -lt ${#args[@]} ]; then
                    FORMAT="${args[$i]}"
                fi
            fi
            ;;
        --cluster|--timeout|--language)
            # Known flags that take a value -- skip the value token
            if [ -z "$val" ]; then i=$((i + 1)); fi
            ;;
        --allow-errors|--no-inject-session|--no-preprocess|--clean)
            # Known boolean flags (no value) -- nothing to skip
            ;;
        -*)
            # Unknown flags (forwarded as-is, assumed no value)
            ;;
        *)
            # First non-flag argument is the notebook path
            if [ -z "$NOTEBOOK" ]; then
                NOTEBOOK="${args[$i]}"
            fi
            ;;
    esac
    i=$((i + 1))
done

# --- Validate early, before any side effects ---

[ -n "$NOTEBOOK" ] || die "notebook path is required (first positional argument)"
[ -f "$NOTEBOOK" ] || die "notebook not found: $NOTEBOOK"
$PROFILE_FOUND || die "--profile is required"

# --- Compute paths ---

STEM="$(basename "$NOTEBOOK")"
STEM="${STEM%.*}"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="tmp/${STEM}-output"
fi

LOG_PATH="$OUTPUT_DIR/$STEM.run.log"

mkdir -p "$OUTPUT_DIR"

# --- Emit machine-readable status block ---
# Written to stderr (visible in agent tool output) and to the log file header.

_status() {
    echo "agent-nb-run: $1" >&2
    echo "agent-nb-run: $1" >> "$LOG_PATH"
}

: > "$LOG_PATH"  # create/truncate log file
_status "notebook=$NOTEBOOK"
_status "output-dir=$OUTPUT_DIR"
_status "log=$LOG_PATH"
_status "stem=$STEM"

# --- Run agent-notebook, tee-ing all output to the log file ---

agent-notebook run "$@" 2>&1 | tee -a "$LOG_PATH"
