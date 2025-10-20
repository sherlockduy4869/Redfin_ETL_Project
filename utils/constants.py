import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.py'))

#AWS EMR
JOB_FLOW_OVERRIDES = parser.get('aws_emr', 'job_flow_overrides')

#SPARK
SPARK_STEPS_EXTRACTION = parser.get('spark', 'spark_steps_extraction')
SPARK_STEPS_TRANSFORMATION = parser.get('spark', 'spark_steps_transformation')

#EMR STATUS
EMR_CLUSTER_WAITING_STATE = "WAITING"
EMR_STEP_COMPLETED_STATE = "COMPLETED"
EMR_CLUSTER_TERMINATED_STATE = "TERMINATED"