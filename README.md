# 🚀 Prefect Orchestration Demo

Pet-проект: оркестрация ETL-пайплайна с использованием **Prefect**. Демонстрирует Python-нативный подход (декораторы `@flow` и `@task`), динамическое выполнение и визуализацию в Prefect UI.

## 📸 Скриншот

**Граф выполнения Flow (Extract → Transform → Load):**

![Prefect Flow](assets/prefect_flow.png)

## 🎯 Зачем это

Prefect — это современный оркестратор, который, в отличие от Airflow, позволяет писать обычные функции Python и оборачивать их в декораторы `@flow` и `@task`. Это делает код более читаемым и гибким. В этом проекте показано, как построить простой ETL-пайплайн (извлечение → очистка → загрузка в PostgreSQL) и визуализировать его в Prefect UI.

## 🏗 Архитектура

```mermaid
graph LR
    Extract["extract_data<br/>(генерация данных)"] -->|DataFrame| Transform["transform_data<br/>(очистка)"]
    Transform -->|DataFrame| Load["load_data<br/>(запись в PostgreSQL)"]
    Load -->|SQL INSERT| DB[("PostgreSQL<br/>public.prefect_demo")]
    
    classDef task fill:#f3e5f5,stroke:#4a148c,stroke-width:2px;
    classDef db fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;
    
    class Extract,Transform,Load task;
    class DB db;
```

## 📂 Структура

```
prefect-orchestration-demo/
├── flows/
│   └── etl_pipeline.py    # ETL-пайплайн на Prefect
├── assets/
│   └── prefect_flow.png   # Скриншот графа
├── README.md
└── requirements.txt
```

## 🚀 Запуск

1. Установи зависимости:
```bash
pip install -r requirements.txt
```

2. Запусти Prefect server (в отдельном окне):
```bash
prefect server start
```

3. Установи переменную окружения (в Windows):
```cmd
set PREFECT_API_URL=http://127.0.0.1:4200/api
```

4. Запусти flow (из корня проекта):
```bash
python flows/etl_pipeline.py
```

5. Открой Prefect UI: `http://localhost:4200`.

## 📊 Результаты

- **Flow успешно выполнен** (2 запуска, все задачи Completed).
- **Данные загружены** в таблицу `public.prefect_demo` (174 строки за два запуска).
- **Граф задач** визуализирован в Prefect UI.

## 🔗 Связанные проекты

- [`airflow-e2e-api-pipeline`](https://github.com/nconichtchenko-arch/airflow-e2e-api-pipeline) — пример на Airflow.
- [`dagster-orchestration-demo`](https://github.com/nconichtchenko-arch/dagster-orchestration-demo) — пример на Dagster.

## 📫 Контакты

- **GitHub:** [github.com/nconichtchenko-arch](https://github.com/nconichtchenko-arch)
- **Email:** pavloni@mail.ru
- **Формат работы:** удалённо
