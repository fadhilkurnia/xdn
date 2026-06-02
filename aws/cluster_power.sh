#!/usr/bin/env bash
# cluster_power.sh - pause/resume the XDN cluster's EC2 instances to cut compute
# cost during idle periods. For an intermittently-used (experiments) cluster this
# is the single biggest lever: STOPPED instances bill only their EBS (~$7/mo
# total) instead of compute (~$150-180/mo for the ARs).
#
# The RC Elastic IP and every node's static private + global IPv6 address persist
# across stop/start, so the cluster resumes at the SAME endpoints -- no DNS glue
# change, no terraform churn (stop/start is not a terraform-managed attribute).
#
# Usage:
#   ./cluster_power.sh pause     # stop all running RC + AR instances
#   ./cluster_power.sh resume    # start all stopped RC + AR instances
#   ./cluster_power.sh status    # show each node's instance type + power state
#
# After `resume`, allow ~1-2 min for the JVM / coredns / Docker units to come up,
# then RE-LAUNCH your services: service state lives under /tmp on the nodes and is
# not guaranteed to survive a stop/start (systemd-tmpfiles may reap it), so treat
# each experiment run as a fresh deploy.
#
# NOTE on Spot: if you enabled `ar_use_spot`, the ARs use PERSISTENT spot with
# `stop` interruption behavior, so this script can still stop/start them. One-time
# spot instances can only be terminated, not stopped.
set -euo pipefail
REGION="${AWS_REGION:-us-east-1}"
FILTER=("Name=tag:Name,Values=xdn-rc,xdn-ar-*")

ids_in_state() {
  aws ec2 describe-instances --region "$REGION" \
    --filters "${FILTER[@]}" "Name=instance-state-name,Values=$1" \
    --query 'Reservations[].Instances[].InstanceId' --output text
}

case "${1:-status}" in
pause)
  ids=$(ids_in_state running)
  [ -z "$ids" ] && { echo "no running XDN instances"; exit 0; }
  echo "stopping: $ids"
  aws ec2 stop-instances --region "$REGION" --instance-ids $ids \
    --query 'StoppingInstances[].{Id:InstanceId,State:CurrentState.Name}' --output table
  ;;
resume)
  ids=$(ids_in_state stopped)
  [ -z "$ids" ] && { echo "no stopped XDN instances"; exit 0; }
  echo "starting: $ids"
  aws ec2 start-instances --region "$REGION" --instance-ids $ids \
    --query 'StartingInstances[].{Id:InstanceId,State:CurrentState.Name}' --output table
  ;;
status)
  aws ec2 describe-instances --region "$REGION" --filters "${FILTER[@]}" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].{Name:Tags[?Key==`Name`]|[0].Value,Type:InstanceType,State:State.Name}' \
    --output table
  ;;
*)
  echo "usage: $0 {pause|resume|status}" >&2
  exit 1
  ;;
esac
