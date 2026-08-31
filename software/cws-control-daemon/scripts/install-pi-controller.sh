#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run as root" >&2
    exit 2
fi

SOURCE_DIR=${1:-.}
CONFIG_PATH=${2:-$SOURCE_DIR/config/two-node.example.json}

test -f "$SOURCE_DIR/pyproject.toml"
test -f "$CONFIG_PATH"
test -f "$SOURCE_DIR/systemd/cws-control.service"
test -f "$SOURCE_DIR/systemd/cws-context.service"
test -f "$SOURCE_DIR/systemd/cws-rate-sweep.service"
test -f "$SOURCE_DIR/systemd/cws-prpl-context.service"
test -f "$SOURCE_DIR/systemd/cws-method-blocks.service"

missing_packages=""
for package in python3-venv python3-setuptools python3-wheel iproute2 iputils-ping iw; do
    if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q 'install ok installed'; then
        missing_packages="$missing_packages $package"
    fi
done
if [ -n "$missing_packages" ]; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y $missing_packages
fi

install -d -m 0755 /opt/cws-control
python3 -m venv --system-site-packages /opt/cws-control/venv
/opt/cws-control/venv/bin/pip install --disable-pip-version-check --no-deps --no-build-isolation "$SOURCE_DIR"
install -d -m 0755 /etc/cws-control
install -m 0644 "$CONFIG_PATH" /etc/cws-control/config.json
getent group cws-control >/dev/null || groupadd --system cws-control
id cws-control >/dev/null 2>&1 || useradd --system --gid cws-control --home-dir /var/lib/cws-control --shell /usr/sbin/nologin cws-control
install -d -o cws-control -g cws-control -m 0750 /var/lib/cws-control
install -m 0644 "$SOURCE_DIR/systemd/cws-control.service" /etc/systemd/system/cws-control.service
install -m 0644 "$SOURCE_DIR/systemd/cws-context.service" /etc/systemd/system/cws-context.service
install -m 0644 "$SOURCE_DIR/systemd/cws-rate-sweep.service" /etc/systemd/system/cws-rate-sweep.service
install -m 0644 "$SOURCE_DIR/systemd/cws-prpl-context.service" /etc/systemd/system/cws-prpl-context.service
install -m 0644 "$SOURCE_DIR/systemd/cws-method-blocks.service" /etc/systemd/system/cws-method-blocks.service
install -m 0644 "$SOURCE_DIR/config/method-blocks.example.json" /etc/cws-control/method-blocks.json
systemctl daemon-reload
systemctl enable cws-context.service cws-control.service cws-prpl-context.service
