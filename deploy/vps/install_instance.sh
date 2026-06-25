#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/vps/install_instance.sh <instance> [source_dir]" >&2
  exit 1
fi

INSTANCE="${1:-}"
SRC_DIR="${2:-$(pwd)}"
BOT_ROOT="${BOT_ROOT:-/opt/p2p-bots}"
ENV_ROOT="${ENV_ROOT:-/etc/p2p-bots}"
BOT_USER="${BOT_USER:-${SUDO_USER:-}}"

if [[ -z "${INSTANCE}" ]]; then
  echo "Usage: sudo bash deploy/vps/install_instance.sh <instance> [source_dir]" >&2
  exit 1
fi

if [[ -z "${BOT_USER}" ]]; then
  echo "Could not determine BOT_USER. Export BOT_USER=<linux-user> and retry." >&2
  exit 1
fi

for cmd in python3 rsync systemctl; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Required command is missing: ${cmd}" >&2
    exit 1
  fi
done

if ! id -u "${BOT_USER}" >/dev/null 2>&1; then
  echo "Linux user '${BOT_USER}' does not exist." >&2
  exit 1
fi

APP_DIR="${BOT_ROOT}/${INSTANCE}/app"
VENV_DIR="${BOT_ROOT}/${INSTANCE}/venv"
ENV_FILE="${ENV_ROOT}/${INSTANCE}.env"
SERVICE_FILE="/etc/systemd/system/p2p-bot-${INSTANCE}.service"

mkdir -p "${APP_DIR}" "${VENV_DIR}" "${ENV_ROOT}"
rsync -a --delete \
  --exclude '.env' \
  --exclude '.venv' \
  --exclude 'data' \
  --exclude 'logs' \
  --exclude '__pycache__' \
  --exclude '.git' \
  --exclude '.DS_Store' \
  "${SRC_DIR}/" "${APP_DIR}/"

mkdir -p "${APP_DIR}/data" "${APP_DIR}/logs"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  python3 -m venv "${VENV_DIR}"
fi

"${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel
"${VENV_DIR}/bin/pip" install -e "${APP_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${APP_DIR}/.env.example" "${ENV_FILE}"
  chmod 600 "${ENV_FILE}"
  chown "${BOT_USER}:${BOT_USER}" "${ENV_FILE}"
fi

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=P2P Arbitrage Bot (${INSTANCE})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${BOT_USER}
Group=${BOT_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${VENV_DIR}/bin/python -m p2p_bot.main
Restart=always
RestartSec=10
TimeoutStopSec=30
KillSignal=SIGTERM
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

chown -R "${BOT_USER}:${BOT_USER}" "${BOT_ROOT}/${INSTANCE}"
chmod 644 "${SERVICE_FILE}"
systemctl daemon-reload

echo
echo "Installed instance: ${INSTANCE}"
echo "App dir: ${APP_DIR}"
echo "Env file: ${ENV_FILE}"
echo "Service: p2p-bot-${INSTANCE}.service"
echo
echo "Next steps:"
echo "  1. Edit env: sudo nano ${ENV_FILE}"
echo "  2. Start bot: sudo systemctl enable --now p2p-bot-${INSTANCE}.service"
echo "  3. Check logs: sudo journalctl -u p2p-bot-${INSTANCE}.service -n 200 --no-pager"
