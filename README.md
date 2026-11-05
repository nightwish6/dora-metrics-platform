# DORA Metrics Platform

<p align="center">
  <img src="docs/screenshots/dashboard.png" alt="DORA Metrics Dashboard" width="1000">
</p>

**Платформа для анализа метрик разработки DORA/Flow** — комплексное решение для мониторинга производительности команд разработки на основе стандартов Google DORA (DevOps Research and Assessment) и Microsoft Flow Analytics.

Платформа собирает данные из GitHub (pull requests, releases, commits, issues, milestones, workflow runs), обрабатывает их с помощью dbt и визуализирует в интерактивном Streamlit-дашборде.

---

## 📊 Что измеряется

### 🏆 Core DORA Metrics
| Метрика | Описание |
|---------|----------|
| **Deployment Frequency** | Частота релизов в месяц — как часто команда выпускает код в продакшен |
| **Lead Time for Changes** | Время от коммита до мержа PR (в часах) — скорость прохождения кодовых изменений |
| **Change Failure Rate** | Процент релизов, вызвавших сбой — показатель качества и стабильности |
| **Time to Restore Service** | Время восстановления сервиса после сбоя (в днях) — скорость реагирования на инциденты |

### 📈 Supporting Metrics
- **Flow**: PR merge rate, PR closed time, assignee coverage, stale PR rate
- **Quality**: Revert rate, GPG verification, bug ratio, comment density
- **Health**: Commit velocity, top author share, on-time milestones

---

## 🏗 Архитектура

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   GitHub API    │───▶│   Airflow DAGs   │───▶│   DuckDB Store  │
│  (PRs, Issues,  │    │  (ETL Pipelines) │    │   (dbt Models)  │
│   Commits, etc.) │    └──────────────────┘    └────────┬────────┘
└─────────────────┘                                       │
                                                          ▼
                                                 ┌─────────────────┐
                                                 │ Streamlit App   │
                                                 │ (Dashboard)     │
                                                 └─────────────────┘
```

### Стек технологий
- **Airflow** — оркестрация ETL-пайплайнов
- **dbt** — трансформация данных (3 слоя: raw → staged → marts)
- **DuckDB** — аналитическая СУБД в файле
- **Streamlit** — веб-дашборд с визуализацией
- **Docker Compose** — контейнеризация всего стека

---

## 🚀 Быстрый старт

### Требования
- Docker и Docker Compose
- GitHub Personal Access Token (для API)

### Установка

1. **Клонируйте репозиторий:**
   ```bash
   git clone https://github.com/nightwish6/dora-metrics-platform.git
   cd dora-metrics-platform
   ```

2. **Настройте окружение:**
   ```bash
   cp .env.example .env
   ```
   Отредактируйте `.env` и добавьте GitHub токен:
   ```
   GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
   ```

3. **Запустите все сервисы:**
   ```bash
   docker compose up --build -d
   ```

### Доступные сервисы

| Сервис | URL | Порт |
|--------|-----|------|
| **Streamlit Dashboard** | http://localhost:8501 | 8501 |
| **Airflow UI** | http://localhost:8080 | 8080 |
| **PostgreSQL** | localhost:5432 | 5432 |

> **Airflow логин по умолчанию:** `admin` / `admin`

---

## 📁 Структура проекта

```
dora-metrics-platform/
├── airflow/                  # Airflow DAGs и конфигурация
│   ├── dags/                 # ETL-пайплайны
│   ├── logs/                 # Логи Airflow
│   └── plugins/              # Кастомные плагины
├── dbt/                      # dbt проекты и модели
│   ├── models/
│   │   ├── 1_raw/            # Сырые данные из GitHub API
│   │   ├── 2_staged/         # Очистка и стандартизация
│   │   └── 3_marts/          # Бизнес-метрики (DORA)
│   └── macros/               # dbt макросы
├── streamlit/                # Веб-дашборд
│   └── app.py                # Основное приложение
├── config/                   # Конфигурационные файлы
├── data/                     # Данные DuckDB
├── tests/                    # Тесты dbt моделей
├── docker-compose.yml        # Оркестрация сервисов
├── Dockerfile                # Образ Airflow
├── Dockerfile.streamlit      # Образ Streamlit
└── requirements-streamlit.txt # Python зависимости
```

---

## 🔧 Настройка GitHub данных

### Создание токена
1. Перейдите в **Settings → Developer settings → Personal access tokens → Tokens (classic)**
2. Нажмите **Generate new token**
3. Выберите области (scopes):
   - `repo` — доступ к репозиториям
   - `read:org` — чтение информации об организации

### Загрузка данных
1. Откройте Airflow: http://localhost:8080
2. Активируйте DAG-файлы в панели слева
3. Запустите пайплайн нажатием **Trigger DAG**
4. Мониторьте прогресс во вкладке **Graph View**

---

## 📊 DASHBOARD

### Визуализация трендов
Каждый график отображает:
- **Цветная линия** — фактические значения метрики по месяцам
- **Серая линия** — тренд линейной регрессии (показывает общее направление)

### Метрики
1. **Deployment Frequency** — количество релизов в месяц
2. **Lead Time** — среднее время от коммита до мержа (часы)
3. **Change Failure Rate** — процент неудачных релизов (%)
4. **Time to Restore** — время восстановления после сбоя (дни)

---

## 🧪 Тестирование

```bash
# Запустить dbt тесты
docker compose exec scheduler dbt test

# Запустить linting
ruff check .
ruff format --check .
```

---

## ⚙️ Настройка

### Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `AIRFLOW_USER` | Логин Airflow | `admin` |
| `AIRFLOW_PASSWORD` | Пароль Airflow | `admin` |
| `GITHUB_TOKEN` | GitHub API токен | — |
| `DUCKDB_PATH` | Путь к DuckDB файлу | `data/duckdb/data.duckdb` |

### Паузы DAGs
По умолчанию DAGs в Airflow создаются в paused-состоянии. Разблокируйте их в UI для запуска.

---

## 🛠 Разработка

### Добавление новой метрики

1. Создайте SQL-модель в `dbt/models/3_marts/`
2. Добавьте запрос данных в `_load_all_data()` в `streamlit/app.py`
3. Добавьте блок визуализации с `st.column()`

### Hot-reload Streamlit
Приложения Streamlit автоматически перезагружаются при изменении файлов. Просто сохраните файл и обновите страницу браузера.

---

## 📝 Лицензия

MIT

---

## 🤝 Вклад

Претензии и pull requests welcome!

---

<div align="center">

**Сделано с ❤️ для команд разработки, которые хотят измерять и улучшать свою производительность**

</div>
