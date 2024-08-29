from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime
import requests
import re

DEFAULT_ARGS = {"email": ["apexdev@sportmaster.ru"],
                "email_on_failure": True}


dag = DAG(
    dag_id="dag_load_linux_core_parameters_info",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
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
                                "WHERE s.PROJECT_NAME = 'load_linux_core_parameters_info' "
                                "ORDER BY s.ID "):
            sqls[str(r[0])] = ''.join(r[1].read())
    return sqls


def _get_kernel():
    PARAMS = ['kernel']
    URL = 'https://www.kernel.org/doc/Documentation/sysctl/'

    for param_prefix in PARAMS:
        req = requests.get(URL + param_prefix + '.txt')

        params_arr = req.text.split('==============================================================')

        params_arr.pop(0)
        params_arr.pop(0)
        params_arr.pop(len(params_arr) - 1)

        params_dict_temp = {a.strip().split('\n')[0].strip(':'): '\n'.join(a.strip().split('\n')[1:]) for a in
                            params_arr}

        params_dict = {}
        keys = []

        for key in params_dict_temp:
            key_tmp = key.split(':')[0]
            keys_tmp = re.split(r'[&,]+|and ', key_tmp)
            for key_tmp in keys_tmp:
                if key_tmp.strip() != '':
                    params_dict[param_prefix + '.' + key_tmp.strip()] = params_dict_temp[key].strip()

        apex_hook = OracleHook(oracle_conn_id="apex")

        with apex_hook.get_conn() as connection_apex:
            sql_list = get_sql_scripts(connection_apex)
            with connection_apex.cursor() as cursor_apex:
                for key in params_dict:
                    cursor_apex.execute(sql_list['LOAD_LINUX_CORE_PARAMETERS_INFO_MERGE'],
                                        param=key,
                                        description=params_dict[key])

                connection_apex.commit()


get_kernel = PythonOperator(
    task_id="kernel",
    python_callable=_get_kernel,
    dag=dag,
)

start >> get_kernel
