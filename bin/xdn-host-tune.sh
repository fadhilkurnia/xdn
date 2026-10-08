#!/bin/bash
# Host latency tuning for XDN nodes. Runs ON the host (needs root or passwordless sudo).
#
#   xdn-host-tune.sh on      keep CPUs out of deep idle states, performance governor,
#                            optional kernel busy polling
#   xdn-host-tune.sh off     revert everything
#   xdn-host-tune.sh status
#
# Why: each XDN request crosses many thread hand-offs (Netty, coordinator, Paxos, NIO
# reader/worker/logger/sender, on every replica). On an idle host every hand-off wakes a
# core from a deep C-state (C6 exit is ~130 us on Xeon E5 v4), which added ~460 us to a
# ~750 us request on CloudLab xl170 (eval/datasets/cloudlab-netlat/2026-10-08-xdn-breakdown).
# Holding /dev/cpu_dma_latency at 0 removes that; it costs idle power, nothing else.
#
# Environment:
#   XDN_HOST_TUNE=0                 skip entirely (callers honour this before invoking us)
#   XDN_HOST_TUNE_BUSY_POLL_US=N    also set net.core.busy_poll/busy_read=N (0 = leave alone)
#   XDN_HOST_TUNE_GOVERNOR=perf     cpufreq governor to set when "on" (default performance)
set -u
PIDFILE=/run/xdn-host-tune.pid
STATEFILE=/run/xdn-host-tune.state
BUSY_POLL_US="${XDN_HOST_TUNE_BUSY_POLL_US:-0}"
GOV="${XDN_HOST_TUNE_GOVERNOR:-performance}"

if [ "$(id -u)" != 0 ]; then
  if sudo -n true 2>/dev/null; then
    exec sudo -n XDN_HOST_TUNE_BUSY_POLL_US="$BUSY_POLL_US" XDN_HOST_TUNE_GOVERNOR="$GOV" "$0" "$@"
  fi
  echo "xdn-host-tune: need root or passwordless sudo; skipping" >&2
  exit 0
fi

holder_alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

do_on() {
  if [ -w /dev/cpu_dma_latency ] && ! holder_alive; then
    # A process must keep the device open for the request to stay in effect.
    setsid bash -c 'exec 3>/dev/cpu_dma_latency; printf "\x00\x00\x00\x00" >&3; echo $$ > '"$PIDFILE"'; exec sleep infinity' \
      </dev/null >/dev/null 2>&1 &
    sleep 0.2
  fi
  if [ -d /sys/devices/system/cpu/cpu0/cpufreq ]; then
    prev=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
    [ -f "$STATEFILE" ] || echo "governor=$prev" > "$STATEFILE"
    for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo "$GOV" > "$g" 2>/dev/null; done
  fi
  if [ "$BUSY_POLL_US" != 0 ]; then
    grep -q busy_poll "$STATEFILE" 2>/dev/null || \
      echo "busy_poll=$(sysctl -n net.core.busy_poll) busy_read=$(sysctl -n net.core.busy_read)" >> "$STATEFILE"
    sysctl -q -w net.core.busy_poll="$BUSY_POLL_US" net.core.busy_read="$BUSY_POLL_US"
  fi
}

do_off() {
  if holder_alive; then kill "$(cat "$PIDFILE")" 2>/dev/null; fi
  rm -f "$PIDFILE"
  if [ -f "$STATEFILE" ]; then
    prev=$(sed -n 's/^governor=//p' "$STATEFILE")
    if [ -n "$prev" ] && [ -d /sys/devices/system/cpu/cpu0/cpufreq ]; then
      for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo "$prev" > "$g" 2>/dev/null; done
    fi
    bp=$(sed -n 's/^busy_poll=\([0-9]*\) .*/\1/p' "$STATEFILE"); br=$(sed -n 's/.*busy_read=\([0-9]*\)/\1/p' "$STATEFILE")
    [ -n "$bp" ] && sysctl -q -w net.core.busy_poll="$bp" net.core.busy_read="${br:-0}"
    rm -f "$STATEFILE"
  fi
}

do_status() {
  echo "host=$(hostname -s) cpu_dma_latency_holder=$(holder_alive && echo on || echo off)" \
    "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)" \
    "busy_poll=$(sysctl -n net.core.busy_poll) busy_read=$(sysctl -n net.core.busy_read)" \
    "deepest_cstate=$(for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do [ "$(cat $s/disable)" = 0 ] && cat $s/name; done | tail -1)"
}

case "${1:-status}" in
  on) do_on; do_status ;;
  off) do_off; do_status ;;
  status) do_status ;;
  *) echo "usage: $0 on|off|status" >&2; exit 2 ;;
esac
