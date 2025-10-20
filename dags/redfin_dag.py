import os
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import EMR_CLUSTER_TERMINATED_STATE, \
                            EMR_CLUSTER_WAITING_STATE, \
                            EMR_STEP_COMPLETED_STATE, \
                            JOB_FLOW_OVERRIDES, \
                            SPARK_STEPS_EXTRACTION, \
                            SPARK_STEPS_TRANSFORMATION

default_args = {
    'owner': 'trandinhduy',
    'start_date': datetime(2025, 10, 19)
}

job_flow_id = "{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}"
step_id = "{{ task_instance.xcom_pull(task_ids='tsk_add_transformation_step')[0] }}"

dag = DAG(
    dag_id='redfin_analytics_spark_job_dag',
    default_args=default_args,
    schedule = '@weekly',
    catchup=False,
    tags = ['redfin', 'etl']
)

start_pipeline = EmptyOperator(
        task_id="tsk_start_pipeline"
        , dag=dag
)


#Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id = "tsk_create_emr_cluster",
    job_flow_overrides = JOB_FLOW_OVERRIDES,
    dag = dag
)

is_emr_cluster_created = EmrJobFlowSensor(
    task_id = "tsk_is_emr_cluster_created", 
    job_flow_id = job_flow_id,
    target_states = {EMR_CLUSTER_WAITING_STATE},  # Specify the desired state
    timeout = 3600,
    poke_interval = 5,
    mode = 'poke',
    dag = dag
)

# Add your steps to the EMR cluster
add_extraction_step = EmrAddStepsOperator(
    task_id = "tsk_add_extraction_step",
    job_flow_id = job_flow_id,
    steps = SPARK_STEPS_EXTRACTION,
    dag = dag
)

is_extraction_completed = EmrStepSensor(
    task_id = "tsk_is_extraction_completed",
    job_flow_id = job_flow_id,
    step_id = "{{ task_instance.xcom_pull(task_ids='tsk_add_extraction_step')[0] }}",
    target_states = {EMR_STEP_COMPLETED_STATE},
    timeout = 3600,
    poke_interval = 5,
    dag = dag
)

add_transformation_step = EmrAddStepsOperator(
    task_id = "tsk_add_transformation_step",
    job_flow_id = job_flow_id,
    steps = SPARK_STEPS_TRANSFORMATION,
    dag = dag
)

is_transformation_completed = EmrStepSensor(
    task_id = "tsk_is_transformation_completed",
    job_flow_id = job_flow_id,
    step_id = step_id,
    target_states = {EMR_STEP_COMPLETED_STATE},
    timeout = 3600,
    poke_interval = 10,
    dag = dag
)

remove_cluster = EmrTerminateJobFlowOperator(
    task_id="tsk_remove_cluster",
    job_flow_id = job_flow_id,
    dag = dag
)

is_emr_cluster_terminated = EmrJobFlowSensor(
    task_id = "tsk_is_emr_cluster_terminated", 
    job_flow_id = job_flow_id,
    target_states = {EMR_CLUSTER_TERMINATED_STATE},  
    timeout = 3600,
    poke_interval = 5,
    mode = 'poke',
    dag = dag
)

end_pipeline = EmptyOperator(
        task_id="tsk_end_pipeline",
        dag=dag
)

# Define task dependencies
start_pipeline >> create_emr_cluster >> is_emr_cluster_created 

is_emr_cluster_created >> add_extraction_step >> is_extraction_completed

is_extraction_completed >> add_transformation_step >> is_transformation_completed

is_transformation_completed >> remove_cluster >> is_emr_cluster_terminated >> end_pipeline
