from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "dagrun_timeout": timedelta(minutes=60),
}

# define the python function
def print_function():
    return "Python Operator print statement"


dag = DAG("snap_shot_update", default_args=default_args, schedule_interval=None)

#start = BashOperator(task_id="deploy_job", bash_command="date", dag=dag)



secret_env1 = Secret('env', 'HOST', 'database-secrets', 'HOST')
secret_env2 = Secret('env', 'USERNAME', 'database-secrets', 'USERNAME')
secret_env3 = Secret('env', 'PORT', 'database-secrets', 'PORT')
secret_env4 = Secret('env', 'DBNAME', 'database-secrets', 'DBNAME')
secret_env5 = Secret('env', 'PASSWORD', 'database-secrets', 'PASSWORD')


passing = KubernetesPodOperator(
    namespace="default",
    image="647612030395.dkr.ecr.eu-west-1.amazonaws.com/test_mcd_etl:v1.2",
    cmds=None,
    arguments=["source.task.snapshot_update.py","source-data/Price_Snapshot_etl_test.txt.gz"],
    # volume_mounts=volume_mount_list,
    # volumes=volume_list,
    labels={"foo": "bar"},
    name="etl-test",
    task_id="etl-task",
    in_cluster=True,
    #config_file=os.path.expanduser('~')+"/.kube/config",
    get_logs=True,
    execution_timeout=timedelta(seconds=600),
    secrets=[secret_env1,secret_env2,secret_env3,secret_env4,secret_env5],
    dag=dag,
)

#passing = PythonOperator(task_id="python_operator_print", python_callable=print_function, dag=dag)
#passing.set_upstream(start)
passing.dry_run()