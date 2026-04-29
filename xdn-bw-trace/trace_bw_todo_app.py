#!/usr/bin/env python3
"""trace_bw_todo_app.py — trace_bw.py with todo-app defaults preset.

Wraps trace_bw.py with read/write/seed flags appropriate for the
`fadhilkurnia/xdn-todo` (services/todo-rs) and `fadhilkurnia/xdn-todo-go`
(services/todo-go) services. The read targets a specific item so the
response payload size stays constant per request — same approach as
trace_bw.py's bookcatalog defaults.

Pre-set defaults (override any on the command line as usual):

    --path                /api/todo/tasks/trace_bw
    --method              GET
    --write-path          /api/todo/tasks
    --write-method        POST
    --write-body          {"item":"trace_bw"}
    --write-content-type  application/json
    --seed-path           /api/todo/tasks
    --seed-body           {"item":"trace_bw"}

The seeding step probes GET /api/todo/tasks/trace_bw on the first
target. If it returns 404 (item not yet present), the driver POSTs the
seed body to /api/todo/tasks, which creates the task with counter=1.
Subsequent GETs on /api/todo/tasks/trace_bw then return 200 with a
constant-sized JSON body.

Usage:
    sudo python3 xdn-bw-trace/trace_bw_todo_app.py \\
        --config conf/gigapaxos.xdn.local.properties \\
        --service todo-eventual \\
        --duration 30 --interval 5 --rate 20

All other trace_bw.py flags (--rate, --duration, --interval, --read-ratio,
--target, --reconfigurator, --warmup, --output, --bpftrace-script,
--timeout, --no-seed) work unchanged.
"""

import sys

import trace_bw

# Defaults to inject if the caller did not already pass these flags.
TODO_DEFAULTS = [
    ("--path", "/api/todo/tasks/trace_bw"),
    ("--method", "GET"),
    ("--write-path", "/api/todo/tasks"),
    ("--write-method", "POST"),
    ("--write-body", '{"item":"trace_bw"}'),
    ("--write-content-type", "application/json"),
    ("--seed-path", "/api/todo/tasks"),
    ("--seed-body", '{"item":"trace_bw"}'),
]


def _has_flag(argv, flag):
    """True if argv already contains `flag` as `--flag value` or `--flag=value`."""
    eq_form = flag + "="
    for arg in argv:
        if arg == flag or arg.startswith(eq_form):
            return True
    return False


def main():
    for flag, value in TODO_DEFAULTS:
        if not _has_flag(sys.argv, flag):
            sys.argv.extend([flag, value])
    return trace_bw.main()


if __name__ == "__main__":
    sys.exit(main())
