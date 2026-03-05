# upload-gsheet

Периодическая загрузка данных из 1С:Элемент в Google Таблицы (водители, автопарк, кураторы).

- Python 3.10+, [Polars](https://polars.rs/), Google Sheets API (service account)
- Один проход по расписанию (cron), без веб-сервера

## Требования

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (рекомендуется) или pip

## Установка

С uv (рекомендуется): в проекте указан Python 3.11 в `.python-version`. При первом запуске uv при необходимости скачает нужную версию:

```bash
uv sync
```

Если 3.11 ещё нет, установите его: `uv python install 3.11`, затем снова `uv sync`.

Без uv:

```bash
pip install -e .
```

## Переменные окружения

Скопируйте `.env.example` в `.env` и заполните значения. Обязательные переменные:

- **ELEMENT_LOGIN**, **ELEMENT_PASSWORD** — учётные данные 1С:Элемент
- **ELEMENT_DRIVERS_URL**, **ELEMENT_CARS_URL** — URL API водителей и машин
- **REPORT_SPREADSHEETS_ID** — ID основной Google-таблицы
- **RANGE_FOR_UPLOAD**, **RANGE_FOR_UPLOAD_DRIVERS** — диапазоны листов (автопарк, водители)
- **GOOGLE_CREDENTIALS_PATH** — путь к JSON ключу service account (по умолчанию `creds.json` в корне)

Опционально: **SUPERVISERS_SPREADSHEET_ID**, **SUPERVISERS_RANGE** — таблица кураторов; **LOG_DIR**, **LOG_FILE** — каталог и имя файла логов.

## Запуск

Один проход (обновление всех таблиц):

```bash
uv run python -m upload_gsheet
```

Либо после установки пакета:

```bash
upload-gsheet
```

Для периодического запуска на сервере настройте планировщик (cron, systemd timer, Task Scheduler) с нужным интервалом. При ошибке основной выгрузки процесс завершается с кодом 1.

## Развёртывание и запуск на сервере Ubuntu

### 1. Подготовка системы

Установите Python 3.11 или новее. С uv достаточно установить Python через uv (uv скачает нужную версию). Либо системно:

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev python3-pip
```

Опционально — [uv](https://docs.astral.sh/uv/install/) для быстрой установки зависимостей:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# перелогиньтесь или: source $HOME/.local/bin/env
```

### 2. Размещение кода

Склонируйте репозиторий или скопируйте проект в каталог на сервере, например `/opt/upload_gsheet`:

```bash
sudo mkdir -p /opt/upload_gsheet
sudo chown "$USER:$USER" /opt/upload_gsheet
# затем скопируйте файлы проекта (git clone, scp, rsync и т.п.)
cd /opt/upload_gsheet
```

### 3. Виртуальное окружение и зависимости

**С uv:**

```bash
cd /opt/upload_gsheet
uv sync
```

**Без uv (pip):**

```bash
cd /opt/upload_gsheet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 4. Конфигурация

- Создайте файл `.env` в корне проекта (по образцу `.env.example`) и заполните переменные (логин/пароль 1С, URL, ID таблиц, диапазоны).
- Положите файл учётных данных Google (service account JSON) в каталог проекта и укажите путь в `GOOGLE_CREDENTIALS_PATH` в `.env` (по умолчанию — `creds.json` в корне).
- При необходимости укажите `LOG_DIR` и `LOG_FILE` для логов (по умолчанию — каталог `logs/` и файл `errors.log`).

Права доступа (чтобы только владелец видел секреты):

```bash
chmod 600 .env creds.json
```

### 5. Проверочный запуск

Один проход выгрузки:

**С uv (без активации venv):**

```bash
cd /opt/upload_gsheet
uv run python -m upload_gsheet
```

**С активированным venv:**

```bash
cd /opt/upload_gsheet
source .venv/bin/activate
python -m upload_gsheet
# или: upload-gsheet
```

Успешное завершение — код 0 и сообщение `DONE` в логах. При ошибке выгрузки — код 1.

### 6. Периодический запуск по расписанию (cron)

Запуск каждые 5 минут от имени пользователя `deploy` (замените на своего):

```bash
crontab -u deploy -e
```

Добавьте строку (путь и интервал при необходимости измените):

```cron
*/5 * * * * cd /opt/upload_gsheet && /opt/upload_gsheet/.venv/bin/python -m upload_gsheet >> /opt/upload_gsheet/logs/cron.log 2>&1
```

Если используете uv без установки пакета в venv:

```cron
*/5 * * * * cd /opt/upload_gsheet && /home/deploy/.local/bin/uv run python -m upload_gsheet >> /opt/upload_gsheet/logs/cron.log 2>&1
```

Каталог для логов создайте заранее:

```bash
mkdir -p /opt/upload_gsheet/logs
```

Логи самого приложения (ошибки и т.п.) пишутся в файл, заданный в `.env` (`LOG_DIR`/`LOG_FILE`).

## Структура проекта

- `src/upload_gsheet/` — пакет приложения
  - `config.py` — конфигурация из `.env`
  - `api/element.py` — клиент 1С:Элемент (Polars)
  - `sheets/client.py` — клиент Google Sheets
  - `formatters/` — форматирование строк
  - `jobs/` — сценарии выгрузки (водители+автопарк, кураторы)
  - `run.py` — точка входа

## Публикация на GitHub

Перед первым push убедитесь:

1. **Секреты не в репозитории:** в коммит не попадают `.env`, `creds.json`, `logs/`, `errors.log` (проверьте `.gitignore`). Файл `.env.example` — без значений, только имена переменных.
2. **Проверка:** `uv sync` и `uv run python -m upload_gsheet` (или `ruff check src`) проходят локально.
3. **Лицензия:** в корне есть `LICENSE` (MIT). При необходимости укажите правообладателя в `LICENSE` и в `pyproject.toml`.

После публикации в настройках репозитория можно включить **Dependabot** для обновления зависимостей и при необходимости **Actions** для проверки кода (ruff).

## Цели проекта

Код предназначен для внутреннего использования: выгрузка актуальных данных из 1С в Google Таблицы по расписанию.
