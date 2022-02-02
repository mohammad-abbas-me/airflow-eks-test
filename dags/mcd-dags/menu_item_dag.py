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
    "retry_delay": timedelta(minutes=5),
    "dagrun_timeout": timedelta(minutes=5),
}

# define the python function
def print_function():
    return "Python Operator print statement"


dag = DAG("deploy_snapshot_dag_new1", default_args=default_args, schedule_interval=None)

#start = BashOperator(task_id="deploy_job", bash_command="date", dag=dag)



secret_env1 = Secret('env', 'HOST', 'database-secrets', 'HOST')
secret_env2 = Secret('env', 'USERNAME', 'database-secrets', 'USERNAME')
secret_env3 = Secret('env', 'PORT', 'database-secrets', 'PORT')
secret_env4 = Secret('env', 'DBNAME', 'database-secrets', 'DBNAME')
secret_env5 = Secret('env', 'PASSWORD', 'database-secrets', 'PASSWORD')


secret_env6 = Secret('env', 'AWS_DEFAULT_REGION', 'aws-secrets', 'AWS_DEFAULT_REGION')
secret_env7 = Secret('env', 'AWS_SECRET_ACCESS_KEY', 'aws-secrets', 'AWS_SECRET_ACCESS_KEY')
secret_env8 = Secret('env', 'AWS_ACCESS_KEY_ID', 'aws-secrets', 'AWS_ACCESS_KEY_ID')

passing = KubernetesPodOperator(
    namespace="default",
    image="647612030395.dkr.ecr.eu-west-1.amazonaws.com/test_mcd_etl:latest",
    cmds=None,
    arguments=["source.task.menu_item.py","source-data/Price_Snapshot_test2.txt"],
    # volume_mounts=volume_mount_list,
    # volumes=volume_list,
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    in_cluster=False,
    config_file=os.path.expanduser('~')+"/.kube/config",
    get_logs=True,
    execution_timeout=timedelta(seconds=600),
    secrets=[secret_env1,secret_env2,secret_env3,secret_env4,secret_env5],
    dag=dag,
)

#passing = PythonOperator(task_id="python_operator_print", python_callable=print_function, dag=dag)
#passing.set_upstream(start)
passing.dry_run()