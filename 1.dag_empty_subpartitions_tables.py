from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
import oracledb
from datetime import datetime

default_args = {
    "email": ["apexdev@sportmaster.ru"],
    "email_on_failure": True
}

dag = DAG(
    dag_id="empty_subpartitions_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    tags=['apex'],
    default_args=default_args
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

    apex_hook = OracleHook(oracle_conn_id="apex")

    with apex_hook.get_conn() as connection_apex:
        sql_list = get_sql_scripts(connection_apex)
        with connection_apex.cursor() as cursor_apex:
            cursor_apex.execute(sql_list['EMPTY_SUBPARTITIONS_TABLES_CLEAR'])
            connection_apex.commit()

            cursor_apex.execute(sql_list['EMPTY_SUBPARTITIONS_TABLES_SELECT_DB'])
            rows = cursor_apex.fetchall()

    for row in rows:
        rows_db = []
        error_text = None
        try:
            with oracledb.connect(user="SYS",
                                  password=row[0],
                                  dsn=row[1],
                                  mode=oracledb.SYSDBA) as connection_db:
                with connection_db.cursor() as cursor_db:
                    cursor_db.execute(sql_list['EMPTY_SUBPARTITIONS_TABLES_SELECT_RESULTS'])
                    rows_db = cursor_db.fetchall()
                    connection_result = 'Y'
        except Exception as e:
            rows_db.clear()
            connection_result = 'N'
            error_text = str(e)

        with apex_hook.get_conn() as connection_apex:
            with connection_apex.cursor() as cursor_apex:
                for row_db in rows_db:
                    cursor_apex.execute(sql_list['EMPTY_SUBPARTITIONS_TABLES_INSERT_RESULTS'],
                                        dbid=row[2],
                                        owner=row_db[0],
                                        segment_name=row_db[1],
                                        partition_name=row_db[2],
                                        segment_type=row_db[3],
                                        num_rows=row_db[4],
                                        segment_size_mb=row_db[5],
                                        index_size_for_table=row_db[6])

                cursor_apex.execute(sql_list['EMPTY_SUBPARTITIONS_TABLES_INSERT_RES_CONN'],
                                    dbid=int(row[2]),
                                    success_connect=str(connection_result),
                                    error_text=error_text)
                connection_apex.commit()


get_info = PythonOperator(
    task_id="get_info",
    python_callable=_get_info,
    dag=dag,
)

start >> get_info
