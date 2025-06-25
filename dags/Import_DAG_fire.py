from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
from pendulum import timezone
import logging
import time
import google.auth
import os
import requests
import pytz
from google.cloud import storage
from io import StringIO

DAG_ID = 'Import_DAG_fire'
#enviroment_dataset = Variable.get('Environment_Dataset')
enviroment_dataset = 'fire_inc_env'
table_log ='Logs'
table_config = "JobsConfig"
system_params = 'SystemParams'
fire_url = 'https://data.sfgov.org/resource/wr8u-xric.json'

def insert_failure_log_into_bq(context, **kwargs):
    """
    Logs the failure of a task to BigQuery.
    This function inserts an error message into the `Log` table when any task fails.
    
    Args:
    context (dict): The Airflow context, which contains information about the task and exception.

    """
    logging.info("Writing error to Log table...")
    logging.info(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

    ti = context['task_instance']
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    processid_runid = ti.xcom_pull(task_ids='get_processid_runid', key='get_processid_runid')
    process_id = processid_runid['process_id']
    run_id = processid_runid['run_id']

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    logs_schema = system_params_dict['2']

    error_message = str(context.get('exception'))
    logging.info(f'Error Message: {error_message}')

    logging.info(f"Importing data with ProcessID: {process_id}, RunID: {run_id}")

    insert_error_query =f"""INSERT INTO `{project_id}.{logs_schema}.{table_log}` (ProcessID, RunID, Level, Info, LogTime)
                    VALUES ({process_id}, '{run_id}', 'Error', 'Import DAG Error', CURRENT_TIMESTAMP())
                    """
    logging.info(f"Insert clause: {insert_error_query}")

    insert_log_task = BigQueryInsertJobOperator(
        task_id='insert_failure_log_into_bq', 
        configuration={
            "query": {
                "query": insert_error_query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        dag=context['dag']
    )
    insert_log_task.execute(context=context)
    logging.info(f"Error log was inserted to {project_id}.{logs_schema}.{table_log} table")

def write_to_log(log_level, log_message, **context):
    
    """
    Insert log entries into the BigQuery log table. If triggered from 'trigger_dag', insert multiple rows,
    otherwise insert a single row.

    Parameters:
    log_level (str): The level of the log (e.g., 'Info', 'Error').
    log_message (str): The log message to be inserted.
    **kwargs: The argument dictionary provided by Airflow, which includes the task instance (ti).
    """

    ti = context['task_instance']
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    processid_runid = ti.xcom_pull(task_ids='get_processid_runid', key='get_processid_runid')
    process_id = processid_runid['process_id']
    run_id = processid_runid['run_id']

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    logs_schema = system_params_dict['2']

    insert_log_query = f"""
    INSERT INTO `{project_id}.{logs_schema}.{table_log}` (ProcessID, RunID, Level, Info, LogTime)
    VALUES ({process_id}, '{run_id}', '{log_level}', '{log_message}', CURRENT_TIMESTAMP())
    """
    logging.info(f"Insert clause: {insert_log_query}")
    insert_log_task = BigQueryInsertJobOperator(
        task_id='insert_log_into_bq',
        configuration={
            "query": {
                "query": insert_log_query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        dag=context['dag']
    )
    insert_log_task.execute(context=context)
    logging.info(f"{log_level} log was inserted to {table_log} table")


def get_gcp_project_id(**kwargs):
    """
    Retrieves the current Google Cloud project ID.

    Returns:
        str: The project ID, or None if an error occurs.
    """
    try:
        credentials, project_id = google.auth.default()
        logging.info(f'Current gcp project id: {project_id}')
        kwargs['ti'].xcom_push(key='project_id', value=project_id)
    except google.auth.exceptions.GoogleAuthError as e:
        logging.error(f"Error obtaining project ID: {e}")
        logging.error("Please ensure that you have authenticated properly. "
              "This usually involves setting the GOOGLE_APPLICATION_CREDENTIALS environment variable "
              "or running 'gcloud auth application-default login'.")

def get_system_params(**kwargs):
    """
    Fetch system parameters from the System_Params BigQuery table and push them to XCom.

    This function queries the System_Params table in BigQuery to retrieve configuration parameters
    (ParamID, ParamName, ParamValue) for the environment. It returns the parameters as a dictionary 
    where the keys are ParamID (as strings) and the values are the corresponding ParamValue.
    The dictionary is also pushed to XCom under the key 'system_params' for use in downstream tasks.

    Parameters:
    **kwargs: Additional keyword arguments provided by Airflow, expected to include 'ti' (task instance).

    Returns:
    dict: A dictionary of system parameters {ParamID: ParamValue}.

    Raises:
    Exception: If the query fails or if no parameters are found.
    """

    project_id = kwargs['ti'].xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    query = f"""
    SELECT ParamID, ParamName, ParamValue
    FROM `{project_id}.{enviroment_dataset}.{system_params}`
    """
    logging.info(f'Executing query: {query}')
    try:
        rows = pandas_gbq.read_gbq(query, project_id=project_id)
        logging.info(f'Query output: {rows.to_string(index=False)}')
        if rows.empty:
            raise Exception(f"No record found in System_Params table")

        rows_dict = rows.to_dict(orient='records')
        logging.info(rows_dict)

        params_dict = {str(param['ParamID']): param['ParamValue'] for param in rows_dict}

        logging.info(f'System Params: {params_dict}')
        kwargs['ti'].xcom_push(key='system_params', value=params_dict)
        return params_dict

    except Exception as e:
        logging.error(f"Error fetching system params: {e}")
        raise Exception(f"Error fetching system params: {e}")
        
def get_processid_runid(**kwargs):

    """
    Retrieves the ProcessID and RunID from the external DAG trigger's configuration.
    These IDs are used for tracking the job and ensuring data consistency.
    
    Args:
    kwargs: context variables provided by Airflow.

    Returns:
    dict: A dictionary containing 'process_id', 'run_id' and 'file_path'
    """
    try:
        process_id = kwargs['dag_run'].conf.get('ProcessID')
        run_id = kwargs['dag_run'].conf.get('RunID')

        process_id = 20
        run_id = 'aaa202020'

        get_processid_runid = {"process_id": process_id, "run_id": run_id}
        logging.info(f"Importing data with ProcessID: {process_id}, RunID: {run_id}")
        kwargs['ti'].xcom_push(key='get_processid_runid', value=get_processid_runid)
        logging.info(f"Pushing to XCom: {get_processid_runid}")
    
    except Exception as e:
        logging.error("Missing ProcessID or RunID.")
        raise Exception("Missing ProcessID or RunID.")

def request_data(**kwargs):
    try:
        print("🔄 Fetching data from API...")
        response = requests.get(fire_url)
        response.raise_for_status()  # Raise error for HTTP errors

        data = response.json()
        df = pd.DataFrame(data)

        print(f"✅ Fetched {len(df)} records")
        kwargs['ti'].xcom_push(key='request_data', value=df)
        logging.info(f"Pushing df to XCom...")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {fire_url}")
        raise Exception(f"❌ Error fetching data: {e}")


def save_df_to_gcs(**kwargs):
    """
    Saves a Pandas DataFrame (from XCom) to a CSV file in a GCS bucket.

    Required XCom:
    - Key: 'request_data' → should be a DataFrame (serialized as JSON)

    Assumes the following params exist in system_params:
    - 'bucket_name'
    - 'gcs_path' (e.g., 'fire_incidents/data_20250624.csv')
    """

    ti = kwargs["ti"]

    df_data = ti.xcom_pull(task_ids='request_data', key='request_data')
    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')

    bucket_name = system_params_dict['6']

    if df_data.empty:
        raise ValueError("❌ No data pulled from XCom for key 'request_data'")

    df = pd.DataFrame(df_data)

    if not bucket_name:
        raise ValueError("❌ 'bucket_name' is missing in system parameters")

    # Convert DF to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("fire_incidents.csv")
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    logging.info(f"✅ CSV uploaded to: gs://{bucket_name}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1, tzinfo=timezone('Asia/Jerusalem')), 
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# Define the DAG
with DAG(
    dag_id= DAG_ID,
    description="This DAG responsible for the orchestration of all DAGs but Import DAG.",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs = 1,
    catchup=False,
    on_success_callback=None
) as dag:
    
    get_gcp_project_id_task = PythonOperator(
        task_id='get_gcp_project_id',
        python_callable=get_gcp_project_id,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        doc_md="""
        ### request_data_task
        """
    )

    get_system_params_task = PythonOperator(
        task_id='get_system_params',
        python_callable=get_system_params,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        doc_md="""
        ### request_data_task
        """
    )

    get_processid_runid_task = PythonOperator(
        task_id='get_processid_runid',
        python_callable=get_processid_runid,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        doc_md="""
        ### request_data_task
        """
    )
    
    request_data_task = PythonOperator(
        task_id='request_data',
        python_callable=request_data,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        doc_md="""
        ### request_data_task
        """
    )

    save_df_to_gcs_task = PythonOperator(
        task_id='import_to_gcs',
        python_callable=save_df_to_gcs,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        doc_md="""
        ### import_to_gcs_task
        """
    )

    write_to_log_start_task = PythonOperator(
            task_id='write_to_log_start',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Import DAG Started'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            doc_md="""
            ### write_to_log_start_task
            """
    )

    write_to_log_end_task = PythonOperator(
            task_id='write_to_log_end',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Import DAG Finished'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            doc_md="""
            ### write_to_log_end_task
            """
    )

    get_gcp_project_id_task >> get_system_params_task >> get_processid_runid_task >> write_to_log_start_task >> request_data_task >> save_df_to_gcs_task >> write_to_log_end_task