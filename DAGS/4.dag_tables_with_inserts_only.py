from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime

DEFAULT_ARGS = {"email": ["apexdev@sportmaster.ru"],
                "email_on_failure": True}

dag = DAG(
    dag_id="tables_with_inserts_only",
    start_date=datetime(2025, 4, 1),
    schedule_interval="00 01 * * 2",
    catchup=False,
    tags=['apex'],
    default_args=DEFAULT_ARGS
)

start = EmptyOperator(task_id="start")


def get_sql_scripts(connection):
    sqls = {}
    with connection.cursor() as cursor:
        for r in cursor.execute("SELECT s.CODE, s.SCRIPT "
                                "FROM APP_APEX_MICROSERVICES.V_PYTHON_SQL s "
                                "WHERE s.PROJECT_NAME = 'tables_with_inserts_only' "
                                "ORDER BY s.ID "):
            sqls[str(r[0])] = ''.join(r[1].read())
    return sqls


def _get_info():
    apex_hook = OracleHook(oracle_conn_id="apex_ms_kafka_tables_insert_consumer")

    with apex_hook.get_conn() as connection_apex:
        sql_list = get_sql_scripts(connection_apex)
        with connection_apex.cursor() as cursor_apex:
            cursor_apex.execute(sql_list['TABLES_WITH_INSERTS_ONLY_QUEUE_DBS'])


get_info = PythonOperator(
    task_id="get_info",
    python_callable=_get_info,
    dag=dag,
)

start >> get_info
