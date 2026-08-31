#!/usr/bin/env bash
set -euo pipefail

target=${CWS_TARGET:?set CWS_TARGET as the prplOS SSH target}
known_hosts=${CWS_TARGET_KNOWN_HOSTS:?set CWS_TARGET_KNOWN_HOSTS}
endpoint=${CWS_ENDPOINT:?set CWS_ENDPOINT}
node_id=${CWS_NODE_ID:?set CWS_NODE_ID}
token_source=${CWS_TOKEN_SOURCE:?set CWS_TOKEN_SOURCE as USER@HOST}
interval=${CWS_INTERVAL_SECONDS:-15}
token=$(ssh -o BatchMode=yes -o "UserKnownHostsFile=$known_hosts" "$token_source" 'sudo cat /var/lib/cws-control/prpl-token')

while true; do
  sampled_epoch_ms=$(( $(date +%s) * 1000 ))
  payload=$(ssh -o BatchMode=yes -o "UserKnownHostsFile=$known_hosts" "$target" 'ubus call system board; ba-cli -j -l -a "WiFi.Radio.2.Status?"; ba-cli -j -l -a "WiFi.AccessPoint.3.AssociatedDeviceNumberOfEntries?"')
  curl --fail --silent --show-error --max-time 10 -H "X-CWS-Token: $token" \
    --data-urlencode "node_id=$node_id" --data-urlencode "sampled_epoch_ms=$sampled_epoch_ms" \
    --data-urlencode "radio=$payload" "$endpoint" >/dev/null || true
  sleep "$interval"
done
