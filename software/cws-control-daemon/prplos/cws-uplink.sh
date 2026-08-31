#!/bin/sh
set -eu

config=/etc/cws-uplink.conf
test -r "$config"
. "$config"
: "${CWS_UPLINK_INTERFACE:?missing CWS_UPLINK_INTERFACE}"
: "${CWS_WPA_CONFIG:?missing CWS_WPA_CONFIG}"
interface=$CWS_UPLINK_INTERFACE
wpa_config=$CWS_WPA_CONFIG
wpa_pidfile=/var/run/cws-uplink-wpa.pid
dhcp_pidfile=/var/run/cws-uplink-dhcp.pid

cleanup() {
    [ -s "$dhcp_pidfile" ] && kill "$(cat "$dhcp_pidfile")" 2>/dev/null || true
    [ -s "$wpa_pidfile" ] && kill "$(cat "$wpa_pidfile")" 2>/dev/null || true
    ip route del default dev "$interface" 2>/dev/null || true
    ip addr flush dev "$interface" scope global 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 "${CWS_RADIO_READY_TIMEOUT:-180}"); do
    if iw dev "$interface" info >/dev/null 2>&1; then break; fi
    sleep 1
done
iw dev "$interface" info >/dev/null 2>&1
ip link set "$interface" up
wpa_supplicant -B -s -P "$wpa_pidfile" -i "$interface" -D nl80211 -c "$wpa_config"
for _ in $(seq 1 "${CWS_ASSOCIATION_TIMEOUT:-60}"); do
    iw dev "$interface" link 2>/dev/null | grep -q '^Connected to ' && break
    sleep 1
done
iw dev "$interface" link 2>/dev/null | grep -q '^Connected to '
udhcpc -f -S -i "$interface" -s /usr/lib/cws-udhcpc.script -p "$dhcp_pidfile" &
wait $!
