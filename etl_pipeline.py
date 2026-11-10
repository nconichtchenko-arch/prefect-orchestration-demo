import pandas as pd
import psycopg2
from prefect import flow, task
from datetime import datetime
import random

# Настройки БД (используем обычное подключение, без изменения wal_level)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "dwh_marketplace",
    "user": "dwh_user",
    "password": "dwh_password",
}

@task
def extract_data():
    """Генерирует сырые данные (имитация)."""
    data = {
        "order_id": range(1, 101),
        "amount": [random.uniform(100, 1000) for _ in range(100)],
        "customer_id": [random.randint(1, 20) for _ in range(100)],
    }
    df = pd.DataFrame(data)
    print(f"Extracted {len(df)} rows")
    return df

@task
def transform_data(df: pd.DataFrame):
    """Очищает данные и считает метрики."""
    # Убираем выбросы (например, amount > 900)
    df_clean = df[df["amount"] <= 900].copy()
    # Добавляем колонку с датой
    df_clean["load_date"] = datetime.now().date()
    print(f"Transformed: {len(df_clean)} rows after cleaning")
    return df_clean

@task
def load_data(df: pd.DataFrame):
    """Сохраняет данные в PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Создаем таблицу, если её нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.prefect_demo (
            order_id INTEGER,
            amount NUMERIC,
            customer_id INTEGER,
            load_date DATE
        )
    """)
    
    # Вставляем данные
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO public.prefect_demo (order_id, amount, customer_id, load_date)
            VALUES (%s, %s, %s, %s)
        """, (row["order_id"], row["amount"], row["customer_id"], row["load_date"]))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} rows to PostgreSQL")
    return len(df)

@flow(name="ETL Pipeline Demo")
def etl_pipeline():
    """Основной flow."""
    raw_data = extract_data()
    clean_data = transform_data(raw_data)
    rows_loaded = load_data(clean_data)
    return rows_loaded

if __name__ == "__main__":
    etl_pipeline()