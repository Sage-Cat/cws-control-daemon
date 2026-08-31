#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ] || [ "$#" -ne 2 ]; then
    echo "usage: printf 'TOKEN\\n' | $0 ENDPOINT NODE_ID" >&2
    exit 2
fi

endpoint=$1
node_id=$2
IFS= read -r token
case "$endpoint" in
    http://*:*'/v1/telemetry') ;;
    *) echo "endpoint must be http://HOST:PORT/v1/telemetry" >&2; exit 2 ;;
esac
if [ "${#token}" -lt 24 ]; then
    echo "token must contain at least 24 characters" >&2
    exit 2
fi
case "$node_id" in
    *[!a-zA-Z0-9._-]*|'') echo "invalid node id" >&2; exit 2 ;;
esac

command -v ba-cli >/dev/null
command -v curl >/dev/null
command -v ubus >/dev/null
cp ./cws-prpl-agent.sh /usr/sbin/cws-prpl-agent
cp ./cws-prpl-agent.init /etc/init.d/cws-prpl-agent
chmod 0755 /usr/sbin/cws-prpl-agent /etc/init.d/cws-prpl-agent
umask 077
printf '%s\n' "$token" >/etc/cws-prpl-agent.token
printf "%s\n" \
    "CWS_ENDPOINT='$endpoint'" \
    "CWS_TOKEN_FILE='/etc/cws-prpl-agent.token'" \
    "CWS_INTERVAL_SECONDS='15'" \
    "CWS_NODE_ID='$node_id'" \
    >/etc/cws-prpl-agent.conf
/etc/init.d/cws-prpl-agent enable
/etc/init.d/cws-prpl-agent restart
