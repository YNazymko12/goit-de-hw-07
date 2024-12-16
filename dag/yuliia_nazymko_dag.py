from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago

# Додавання підтримки PyMySQL
import pymysql
pymysql.install_as_MySQLdb()

# Функція для примусового встановлення статусу на SUCCESS
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)


# Функція для випадкового вибору медалі
def random_medal_choice():
    return random.choice(["Gold", "Silver", "Bronze"])


# Функція затримки виконання
def delay_execution():
    time.sleep(20)


# Базові налаштування для DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# Назва з'єднання для MySQL
mysql_connection_id = "mysql_connection_yuliia"

# Опис DAG
with DAG(
    "yuliia_nazymko_dag",
    default_args=default_args,
    schedule_interval=None,  # DAG не має регулярного розкладу
    catchup=False,  # Вимкнути пропущені виконання
    tags=["yuliia_medal_counting"],
) as dag:

    # Завдання 1: Створення таблиці для збереження результатів
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.yuliia_nazymko_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання 2: Випадковий вибір медалі
    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    # Завдання 3: Розгалуження для підрахунку медалей
    def branching_logic(**kwargs):
        selected_medal = kwargs["ti"].xcom_pull(task_ids="select_medal")
        if selected_medal == "Gold":
            return "count_gold_medals"
        elif selected_medal == "Silver":
            return "count_silver_medals"
        else:
            return "count_bronze_medals"

    branching_task = BranchPythonOperator(
        task_id="branch_based_on_medal",
        python_callable=branching_logic,
        provide_context=True,
    )

    # Завдання 4: Підрахунок медалей (Bronze)
    count_bronze_task = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.yuliia_nazymko_medal_counts (medal_type, medal_count)
           SELECT 'Bronze', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    # Завдання 5: Підрахунок медалей (Silver)
    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.yuliia_nazymko_medal_counts (medal_type, medal_count)
           SELECT 'Silver', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    # Завдання 6: Підрахунок медалей (Gold)
    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.yuliia_nazymko_medal_counts (medal_type, medal_count)
           SELECT 'Gold', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Gold';
           """,
    )

    # Завдання 7: Затримка виконання для симуляції обробки
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # Виконується, якщо хоча б одне попереднє завдання успішне
    )

    # Завдання 8: Перевірка останніх записів за допомогою сенсора
    check_last_record_task = SqlSensor(
        task_id="verify_recent_record",
        conn_id=mysql_connection_id,
        sql="""
                    WITH count_in_medals AS (
                        select COUNT(*) as nrows FROM neo_data.yuliia_nazymko_medal_counts
                        WHERE created_at >= NOW() - INTERVAL 30 SECOND
                        )
                    SELECT nrows > 0 from count_in_medals; 
                """,
        mode="poke",  # Периодична перевірка умови
        poke_interval=10,  # Перевірка кожні 5 секунд
        timeout=30,  # Тайм-аут після 6 секунд
    )

    # Визначення залежностей
    create_table_task >> select_medal_task >> branching_task
    (
        branching_task
        >> [count_bronze_task, count_silver_task, count_gold_task]
        >> delay_task
    )
    delay_task >> check_last_record_task