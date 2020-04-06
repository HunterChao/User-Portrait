from airflow.operators.bash_operator import BashOperator
import airflow
from airflow.models import DAG
from airflow import operators
from airflow.contrib.hooks import SSHHook
from airflow.models import BaseOperator
from airflow.contrib.operators import SSHExecuteOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
import os
import sys
from datetime import timedelta,date,datetime
#import pendulum
from airflow.utils.trigger_rule import TriggerRule

#today=date.today() 
#oneday=timedelta(days=1) 
#yesterday=today-oneday  
#datestr = yesterday.strftime("%Y%m%d")

#local_tz = pendulum.timezone("Asia/Shanghai")

default_args = {
    'owner': 'userprofile',
    'depends_on_past': False,
    'start_date': datetime(2019, 01, 01),
    'email': ['6666@qq.com','',''],
    'email_on_failure': True ,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
os.environ['SPARK_HOME'] = '/usr/local/spark-2.1.1-bin-hadoop2.6'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))


dag = DAG(
    'userprofile',
    default_args=default_args,
    description='userprofile ',
    schedule_interval='00 08  * * *'
)

#---------------------------------------------------------
check_table_finish = BashOperator(
    task_id='check_table_finish_task',
    retry_delay=timedelta(minutes=5),
    retries=6*12,
    bash_command='python /airflow/myscripts/userprofile/check_table_finish.py',
    dag=dag)



# cookie task
cookieid_feature_task = BashOperator(
    task_id='cookieid_feature',
    bash_command=' sudo -E -H -u userprofile spark-submit   --master yarn --deploy-mode client  --driver-memory 1g  --executor-memory 2g --executor-cores 2 --num-executors 50  /airflow/myscripts/userprofile/userprofile_cookieid_feature.py  {{ ds_nodash }} ',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

cookieid_feature_zone_task = BashOperator(
    task_id='cookieid_feature_zone',
    bash_command='python  /airflow/myscripts/userprofile/userprofile_cookieid_feature_zone.py  {{ ds_nodash }} ',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)


last_behavior_days_cookie_task = BashOperator(
    task_id='last_behavior_days',
    bash_command=' sudo -E -H -u userprofile spark-submit   --master yarn --deploy-mode client   --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 30  /airflow/myscripts/userprofile/userprofile_cookieid_last_behavior_days.py  {{ ds_nodash }} ',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)


# user task 
paidinfo_task = BashOperator(
    task_id='userid_paidinfo',
    bash_command=' sudo -E -H -u userprofile spark-submit   --master yarn --deploy-mode client  --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /airflow/myscripts/userprofile/userprofile_userid_paidinfo.py  {{ ds_nodash }} ',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

regist_not_paid_task = BashOperator(
    task_id='regist_not_paid',
    bash_command=' sudo -E -H -u userprofile spark-submit   --master yarn --deploy-mode client  --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /airflow/myscripts/userprofile/userprofile_userid_regist_not_paid.py  {{ ds_nodash }} ',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

loss_recall_task = BashOperator(
    task_id='loss_recall',
    bash_command=' sudo -E -H -u userprofile spark-submit   --master yarn --deploy-mode client  --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /airflow/myscripts/userprofile/userprofile_userid_loss_recall.py  {{ ds_nodash }} >> /logs/userprofile/hbase/userid_loss_recall_{{ ds_nodash }}.log 2>&1 ',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

#----------------------- task 依赖关系

check_table_finish >> cookieid_feature_task
check_table_finish >> paidinfo_task

# cookie flow
cookieid_feature_task >> cookieid_feature_zone_task >> last_behavior_days_cookie_task


# user flow
paidinfo_task >> regist_not_paid_task >> loss_recall_task

