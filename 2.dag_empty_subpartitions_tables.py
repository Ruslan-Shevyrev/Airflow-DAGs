from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime
from kafka import KafkaProducer

DEFAULT_ARGS = {"email": ["apexdev@sportmaster.ru"],
                "email_on_failure": True}

KAFKA_BOOTSTRAP_SERVER = 'oraapex-draft:9092'
TOPIC = 'EMPTY_SUBPARTITIONS_TABLES'

dag = DAG(
    dag_id="empty_subpartitions_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0 1 * *",
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
                                "WHERE s.PROJECT_NAME = 'empty_subpartitions_tables' "
                                "ORDER BY s.ID "):
            sqls[str(r[0])] = ''.join(r[1].read())
    return sqls


def _get_info():

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

    apex_hook = OracleHook(oracle_conn_id="apex")

    with apex_hook.get_conn() as connection_apex:
        sql_list = get_sql_scripts(connection_apex)
        with connection_apex.cursor() as cursor_apex:
            cursor_apex.execute(sql_list['EMPTY_SUBPARTITIONS_TABLES_SELECT_DB'])
            rows = cursor_apex.fetchall()

    for row in rows:
        value = '{ "DBID" : ' + str(row[0]) + '}'
        producer.send(topic=TOPIC,
                      value=bytes(value, 'utf-8'))
        producer.flush()


get_info = PythonOperator(
    task_id="get_info",
    python_callable=_get_info,
    dag=dag,
)

start >> get_info
