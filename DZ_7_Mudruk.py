from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from random import choice
from datetime import datetime
import time

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_yanamud"

# Параметри DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
    'retries': 1,
    'retry_delay': 5,
}

# Визначення DAG
with DAG(
        'mysql_db_ym',
        default_args=default_args,
        schedule=None,
        catchup=False,
        tags=["yana_mud"]
) as dag:

    # 1. Створення бази даних
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        sql="CREATE DATABASE IF NOT EXISTS yana_mud;",
        conn_id=connection_name
    )

    # 2. Створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        sql=""" 
            CREATE TABLE IF NOT EXISTS yana_mud.medal_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                count INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        conn_id=connection_name
    )

    # 3. Випадковий вибір медалі
    def choose_medal(**kwargs):
        medal = choice(['Bronze', 'Silver', 'Gold'])
        print(f"Вибрано медаль: {medal}")
        return f'calc_{medal}'  # Повертає task_id

    branch_task = BranchPythonOperator(
        task_id='pick_medal',
        python_callable=choose_medal
    )

    # 4. Динамічне створення завдань SQLExecuteQueryOperator для кожної медалі
    medal_types = ['Bronze', 'Silver', 'Gold']
    prepare_and_execute_query_tasks = []  # Список для зберігання завдань

    for medal in medal_types:
        query = f"""
            INSERT INTO yana_mud.medal_summary (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = '{medal}';
        """
        # Створюємо окреме завдання SQLExecuteQueryOperator для кожного типу медалі
        medal_task = SQLExecuteQueryOperator(
            task_id=f'calc_{medal}',
            sql=query,
            conn_id=connection_name
        )
        prepare_and_execute_query_tasks.append(medal_task)

        # Зв'язуємо завдання вибору медалі з відповідними завданнями виконання
        branch_task >> medal_task

    # 5. Затримка виконання
    def delay_execution():
        time.sleep(35)

    delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=delay_execution,
        trigger_rule=tr.ONE_SUCCESS  # Виконується, якщо хоча б одне попереднє завдання успішне
    )

    # Зв'язуємо всі завдання медалей із завданням затримки
    for task in prepare_and_execute_query_tasks:
        task >> delay_task

    # 6. Сенсор для перевірки запису
    sensor_task = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql=""" 
                SELECT 1
                FROM yana_mud.medal_summary
                WHERE created_at = (
                    SELECT MAX(created_at)
                    FROM yana_mud.medal_summary
                ) 
                AND created_at >= '{{ data_interval_end }}' - INTERVAL 30 SECOND
                LIMIT 1;
            """,
        timeout=60,
        poke_interval=10,
        mode='poke'
    )

    # 7. Завершальне завдання
    def finish_execution():
        print("DAG виконано успішно!")

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=finish_execution
    )

    # Зв'язки завдань
    create_schema >> create_table >> branch_task
    delay_task >> sensor_task >> end_task