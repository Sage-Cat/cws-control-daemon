#!/bin/sh
set -eu

config=/etc/cws-prpl-agent.conf
test -r "$config"
. "$config"

: "${CWS_ENDPOINT:?missing CWS_ENDPOINT}"
: "${CWS_TOKEN_FILE:?missing CWS_TOKEN_FILE}"
: "${CWS_INTERVAL_SECONDS:=15}"
: "${CWS_NODE_ID:=prplos-node}"

query() {
    expression=$1
    output=$(ba-cli -j -l -a "$expression" </dev/null 2>&1) || true
    printf '%s\n' "$output"
}

while true; do
    token=$(tr -d '\r\n' <"$CWS_TOKEN_FILE")
    sampled_epoch_ms=$(( $(date +%s) * 1000 ))
    board=$(ubus call system board 2>&1 || true)
    radio=$(
        query 'WiFi.Radio.2.Status?'
        query 'WiFi.Radio.2.Channel?'
        query 'WiFi.Radio.2.OperatingChannelBandwidth?'
        query 'WiFi.Radio.2.ChannelLoad?'
        query 'WiFi.Radio.2.Interference?'
        query 'WiFi.Radio.2.Stats.Noise?'
        query 'WiFi.Radio.2.Stats.ErrorsSent?'
        query 'WiFi.Radio.2.Stats.ErrorsReceived?'
        query 'WiFi.Radio.2.Stats.RetransCount?'
        query 'WiFi.Radio.2.Stats.DiscardPacketsSent?'
        query 'WiFi.Radio.2.Stats.WmmFailedSent.?'
    )
    stations=$(query 'WiFi.AccessPoint.3.AssociatedDeviceNumberOfEntries?')
    if ! curl --fail --silent --show-error --max-time 15 \
        -H "X-CWS-Token: $token" \
        --data-urlencode "node_id=$CWS_NODE_ID" \
        --data-urlencode 'agent_version=1.1.0' \
        --data-urlencode "sampled_epoch_ms=$sampled_epoch_ms" \
        --data-urlencode "board=$board" \
        --data-urlencode "radio=$radio" \
        --data-urlencode "stations=$stations" \
        "$CWS_ENDPOINT" >/dev/null; then
        logger -t cws-prpl-agent "telemetry upload failed"
    fi
    sleep "$CWS_INTERVAL_SECONDS"
done
