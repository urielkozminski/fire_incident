from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
from datetime import datetime, timedelta
from pendulum import timezone
import logging
import pandas_gbq
import time
import google.auth
import os
import pytz
from google.cloud import storage
from io import StringIO
from Configs.config import enviroment_dataset, table_log, system_params

DAG_ID = 'Ingestion_DAG_fire'
local_timezone = pytz.timezone('Asia/Jerusalem')

def log_message(level, message, context=None):
    timestamp = datetime.utcnow().isoformat()
    task = context['task_instance'] if context else None
    task_info = f"[{task.task_id} | {task.dag_id}]" if task else ""
    full_message = f"[{timestamp}] [{level}] {task_info} {message}"

    if level == 'INFO':
        logging.info(full_message)
    elif level == 'ERROR':
        logging.error(full_message)
    elif level == 'WARNING':
        logging.warning(full_message)

def on_task_success(context):
    task_id = context['task_instance'].task_id
    duration = context['task_instance'].duration
    try_number = context['task_instance'].try_number
    log_message("INFO", f"✅ Task {task_id} succeeded in {duration:.2f}s (Retries: {try_number})", context)

def insert_failure_log_into_bq(context, **kwargs):
    """
    Logs the failure of a task to BigQuery.
    This function inserts an error message into the `Log` table when any task fails.
    
    Args:
    context (dict): The Airflow context, which contains information about the task and exception.

    """
    log_message("INFO", "Writing error to Log table...", context)
    log_message("ERROR", f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}", context)

    ti = context['task_instance']
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    processid_runid = ti.xcom_pull(task_ids='get_processid_runid', key='get_processid_runid')
    process_id = processid_runid['process_id']
    run_id = processid_runid['run_id']

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    logs_schema = system_params_dict['2']

    error_message = str(context.get('exception'))
    log_message("ERROR", f"Error Message: {error_message}", context)
    log_message("INFO", f"Importing data with ProcessID: {process_id}, RunID: {run_id}", context)

    insert_error_query =f"""INSERT INTO `{project_id}.{logs_schema}.{table_log}` (ProcessID, RunID, Level, Info, LogTime)
                    VALUES ({process_id}, '{run_id}', 'Error', 'Ingestion DAG Error', CURRENT_TIMESTAMP())
                    """
    log_message("INFO", f"Insert clause: {insert_error_query}", context)

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
    log_message("INFO", f"Error log was inserted to {project_id}.{logs_schema}.{table_log} table", context)


def write_to_log(log_level, log_message_text, **context):
    
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
    VALUES ({process_id}, '{run_id}', '{log_level}', '{log_message_text}', CURRENT_TIMESTAMP())
    """
    log_message("INFO", f"Insert clause: {insert_log_query}", context)
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
    log_message(log_level, f"{log_level} log was inserted to {table_log} table", context)

def get_gcp_project_id(**kwargs):
    """
    Retrieves the current Google Cloud project ID.

    Returns:
        str: The project ID, or None if an error occurs.
    """
    try:
        credentials, project_id = google.auth.default()
        log_message("INFO", f"Current gcp project id: {project_id}", kwargs)
        kwargs['ti'].xcom_push(key='project_id', value=project_id)
    except google.auth.exceptions.GoogleAuthError as e:
        log_message("ERROR", f"Error obtaining project ID: {e}", kwargs)

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
    log_message("INFO", f"Executing query: {query}", kwargs)
    try:
        rows = pandas_gbq.read_gbq(query, project_id=project_id)
        log_message("INFO", f"Query output: {rows.to_string(index=False)}", kwargs)
        if rows.empty:
            raise Exception(f"No record found in System_Params table")

        rows_dict = rows.to_dict(orient='records')
        log_message("INFO", str(rows_dict), kwargs)

        params_dict = {str(param['ParamID']): param['ParamValue'] for param in rows_dict}

        log_message("INFO", f"System Params: {params_dict}", kwargs)
        kwargs['ti'].xcom_push(key='system_params', value=params_dict)
        return params_dict

    except Exception as e:
        log_message("ERROR", f"Error fetching system params: {e}", kwargs)
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

        process_id = 10
        run_id = 'aaa101010'

        get_processid_runid = {"process_id": process_id, "run_id": run_id}
        log_message("INFO", f"Importing data with ProcessID: {process_id}, RunID: {run_id}", kwargs)
        kwargs['ti'].xcom_push(key='get_processid_runid', value=get_processid_runid)
        log_message("INFO", f"Pushing to XCom: {get_processid_runid}", kwargs)
    except Exception as e:
        log_message("ERROR", "Missing ProcessID or RunID.", kwargs)
        raise Exception("Missing ProcessID or RunID.")

def import_gcs_to_bq(**kwargs):
    """
    Saves a Pandas DataFrame (from XCom) to a CSV file in a GCS bucket.

    Required XCom:
    - Key: 'request_data' → should be a DataFrame (serialized as JSON)

    Assumes the following params exist in system_params:
    - 'bucket_name'
    - 'gcs_path' (e.g., 'fire_incidents/data_20250624.csv')
    """

    ti = kwargs["ti"]

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    fire_bronze = system_params_dict['3']
    bucket_name = system_params_dict['6']

    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    try:
        load_csv_to_bigquery = GCSToBigQueryOperator(
            task_id='load_csv_to_bigquery',
            bucket=bucket_name,
            source_objects=["fire_incidents.csv"],
            destination_project_dataset_table=f"{project_id}.{fire_bronze}.fire_incident_stg",
            source_format='CSV',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            field_delimiter=",",
            dag=kwargs['dag']
        )
        load_csv_to_bigquery.execute(context=kwargs)
    except Exception as e:
        log_message("ERROR", f"Error Importing CSV to BigQuery: {e}", kwargs)
        raise Exception(f"Error Importing CSV to BigQuery: {e}")
    log_message("INFO", "Loading Succeed!", kwargs)

def depuplicated_table(**kwargs):
     
    ti = kwargs["ti"]

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    fire_bronze = system_params_dict['3']

    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    query_deduplication = f'''

    MERGE `{project_id}.{fire_bronze}.fire_incident` AS T
    USING (
        SELECT
        incident_number,
        exposure_number,
        id,
        address,
        incident_date,
        call_number,
        alarm_dttm,
        arrival_dttm,
        close_dttm,
        city,
        zipcode,
        battalion,
        station_area,
        box,
        suppression_units,
        suppression_personnel,
        ems_units,
        ems_personnel,
        other_units,
        other_personnel,
        fire_fatalities,
        fire_injuries,
        civilian_fatalities,
        civilian_injuries,
        number_of_alarms,
        primary_situation,
        mutual_aid,
        action_taken_primary,
        property_use,
        no_flame_spread,
        supervisor_district,
        neighborhood_district,
        point,
        data_as_of,
        data_loaded_at,
        action_taken_secondary,
        estimated_property_loss,
        area_of_fire_origin,
        ignition_cause,
        ignition_factor_primary,
        heat_source,
        item_first_ignited,
        human_factors_associated_with_ignition,
        estimated_contents_loss,
        action_taken_other,
        structure_type,
        structure_status,
        floor_of_fire_origin,
        detectors_present,
        detector_type,
        detector_operation,
        detector_effectiveness,
        automatic_extinguishing_system_present,
        automatic_extinguishing_sytem_type,
        automatic_extinguishing_sytem_perfomance,
        number_of_sprinkler_heads_operating,
        detector_alerted_occupants,
        row_num
    FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY incident_date DESC) AS row_num
        FROM `{project_id}.{fire_bronze}.fire_incident_stg`
    )
    WHERE row_num = 1
        ) AS S
        ON T.id = S.id
        WHEN NOT MATCHED THEN
        INSERT ROW;

    '''

    try:
        deduplication_table = BigQueryInsertJobOperator(
            task_id='deduplicate_and_merge',
            configuration={
                "query": {
                    "query": query_deduplication,
                    "useLegacySql": False
                }
            },
            gcp_conn_id='google_cloud_default',
        )
        deduplication_table.execute(context=kwargs)
    except Exception as e:
        log_message("ERROR", f"Error Deduplicating Table: {e}", kwargs)
        raise Exception(f"Error Deduplicating Table: {e}")
    log_message("INFO", "Deduplication Loading Succeed!", kwargs)

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
            ### get_gcp_project_id_task
            """
    )

    write_to_log_start_task = PythonOperator(
            task_id='write_to_log_start',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Ingestion DAG Started'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### get_gcp_project_id_task
            """
    )

    get_processid_runid_task = PythonOperator(
            task_id='get_processid_runid',
            python_callable=get_processid_runid,
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### get_processid_runid_task
            """
    )

    get_system_params_task = PythonOperator(
            task_id='get_system_params',
            python_callable=get_system_params,
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### get_system_params_task
            """
    )

    import_gcs_to_bq_task = PythonOperator(
        task_id='import_gcs_to_bq',
        python_callable=import_gcs_to_bq,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success,
        doc_md="""
        ### import_gcs_to_bq_task
        """
    )

    depuplicated_table_task = PythonOperator(
        task_id='depuplicated_table',
        python_callable=depuplicated_table,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success,
        doc_md="""
        ### depuplicated_table_task
        """
    )

    write_to_log_end_task = PythonOperator(
            task_id='write_to_log_end',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Ingestion DAG Finished'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### get_gcp_project_id_task
            """
    )


    get_gcp_project_id_task >> get_processid_runid_task >> get_system_params_task >> write_to_log_start_task >> import_gcs_to_bq_task >> depuplicated_table_task >> write_to_log_end_task