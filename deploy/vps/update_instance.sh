#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/vps/update_instance.sh <instance> [source_dir]" >&2
  exit 1
fi

INSTANCE="${1:-}"
SRC_DIR="${2:-$(pwd)}"
BOT_ROOT="${BOT_ROOT:-/opt/p2p-bots}"

if [[ -z "${INSTANCE}" ]]; then
  echo "Usage: sudo bash deploy/vps/update_instance.sh <instance> [source_dir]" >&2
  exit 1
fi

APP_DIR="${BOT_ROOT}/${INSTANCE}/app"
VENV_DIR="${BOT_ROOT}/${INSTANCE}/venv"
SERVICE_NAME="p2p-bot-${INSTANCE}.service"

if [[ ! -d "${APP_DIR}" ]]; then
  echo "Instance app dir does not exist: ${APP_DIR}" >&2
  exit 1
fi

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Instance virtualenv does not exist: ${VENV_DIR}" >&2
  exit 1
fi

rsync -a --delete \
  --exclude '.env' \
  --exclude '.venv' \
  --exclude 'data' \
  --exclude 'logs' \
  --exclude '__pycache__' \
  --exclude '.git' \
  --exclude '.DS_Store' \
  "${SRC_DIR}/" "${APP_DIR}/"

"${VENV_DIR}/bin/pip" install -e "${APP_DIR}"

if systemctl is-active --quiet "${SERVICE_NAME}"; then
  systemctl restart "${SERVICE_NAME}"
else
  echo "Service ${SERVICE_NAME} is not active. Code updated, service not restarted."
fi

echo "Updated instance: ${INSTANCE}"
echo "Check logs: sudo journalctl -u ${SERVICE_NAME} -n 200 --no-pager"
