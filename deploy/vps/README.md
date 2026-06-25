# VPS Deployment

Этот набор нужен, чтобы посадить бота на VPS отдельным инстансом и не задеть другие сервисы.

Что делает установка:

- создаёт отдельную папку `/opt/p2p-bots/<instance>`
- создаёт отдельный `venv`
- создаёт отдельный `systemd`-сервис `p2p-bot-<instance>.service`
- использует отдельный env-файл `/etc/p2p-bots/<instance>.env`
- не трогает другие unit-файлы и не перезапускает чужие боты

## Зависимости на VPS

Перед установкой убедитесь, что на VPS есть:

- `python3`
- `python3-venv`
- `rsync`
- `systemd`

Для Ubuntu/Debian обычно достаточно:

```bash
sudo apt update
sudo apt install -y python3 python3-venv rsync
```

## Быстрый сценарий

1. Скопируйте этот проект на VPS в любую временную директорию, например:

```bash
scp -r "/local/path/P2P bot" user@vps:/tmp/p2p-bot-src
```

2. На VPS выполните установку:

```bash
cd /tmp/p2p-bot-src
sudo bash deploy/vps/install_instance.sh p2p-arb
```

3. Откройте env-файл и заполните секреты:

```bash
sudo nano /etc/p2p-bots/p2p-arb.env
```

4. Проверьте env и только потом включите сервис:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now p2p-bot-p2p-arb.service
sudo systemctl status p2p-bot-p2p-arb.service
```

## Обновление кода без затрагивания других ботов

После заливки новой версии проекта:

```bash
cd /tmp/p2p-bot-src
sudo bash deploy/vps/update_instance.sh p2p-arb
```

Скрипт обновит только один инстанс и перезапустит только его сервис.

## Полезные команды

```bash
sudo systemctl status p2p-bot-p2p-arb.service
sudo journalctl -u p2p-bot-p2p-arb.service -n 200 --no-pager
sudo journalctl -u p2p-bot-p2p-arb.service -f
```

## Параллельный контур по другому активу

Если нужен второй безопасный инстанс, например `USDC`, поднимайте его отдельным сервисом:

```bash
cd /tmp/p2p-bot-src
BOT_USER=root bash deploy/vps/install_instance.sh p2p-usdc /tmp/p2p-bot-src
sudo cp deploy/vps/p2p-usdc.env.example /etc/p2p-bots/p2p-usdc.env
sudo systemctl enable --now p2p-bot-p2p-usdc.service
```

Что важно для параллельного контура:

- отдельный `systemd` unit
- отдельные `data/` и `logs/`
- `P2P_ASSET=USDC`
- `TELEGRAM_ENABLE_POLLING=false`, чтобы не было `409 getUpdates`
- на старте лучше оставить только `BINANCE,BYBIT` и режим alert-only

## Структура на VPS

```text
/opt/p2p-bots/p2p-arb/
  app/
    p2p_bot/
    pyproject.toml
    README.md
    data/
    logs/
  venv/

/etc/p2p-bots/p2p-arb.env
/etc/systemd/system/p2p-bot-p2p-arb.service
```

## Замечания по безопасности

- Сервис запускается от текущего пользователя, от имени которого вы вызывали `sudo`.
- `.env` из локального Mac не копируется автоматически в VPS.
- `data/` и `logs/` живут внутри конкретного инстанса и не смешиваются с другими ботами.
