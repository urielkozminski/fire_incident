from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
import json
import pandas_gbq
import pandas as pd
import logging
import time
import google.auth
from croniter import croniter
from datetime import datetime, timezone as dt_timezone
from datetime import datetime, timedelta

DAG_ID = 'Master_DAG_fire'

enviroment_dataset = 'fire_inc_env'
table_log ='Logs'
table_config = "JobsConfig"
system_params = 'SystemParams'

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

def generate_run_id(process_id ,**kwargs):
    """
    Generate a unique run ID based on the current epoch time and process ID.

    This function generates a run ID by concatenating the process ID with the current epoch time.
    The generated run ID is logged and returned.

    Returns:
    str: The generated run ID.
    """
    epoch = int(time.time())
    context = kwargs.get('context')
    log_message('INFO', f'epoch unix: {epoch}', context)
    run_id = f'{str(process_id)}{str(epoch)}'
    log_message('INFO', f'Run ID: {run_id}', context)
    return run_id

def write_to_log(log_level, log_message_text, **context):
    """
    Insert log entries into the BigQuery log table. If triggered from 'trigger_dag', insert multiple rows,
    otherwise insert a single row.

    Parameters:
    log_level (str): The level of the log (e.g., 'Info', 'Error').
    log_message_text (str): The log message to be inserted.
    **context: The argument dictionary provided by Airflow, which includes the task instance (ti).
    """
    ti = context['task_instance']
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    process_id = 1
    run_id = generate_run_id(process_id, context=context)

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

def get_jobs_to_trigger(**kwargs):
    """
    Consulta JobsConfig y Logs, y retorna la lista de DAGs que deben ser ejecutados ahora,
    segun su cron y ultimo LogTime.
    """
    ti = kwargs['ti']
    context = kwargs
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    logs_schema = system_params_dict['2']

    now = datetime.utcnow().replace(tzinfo=dt_timezone.utc)

    query_jobs = f"""
    SELECT ProcessID, TriggerCron, ProcessName
    FROM `me-sb-dgcp-dpoc-data-pr.fire_inc_processes.JobsConfig`
    WHERE IsActive = TRUE
    """
    jobs_df = pandas_gbq.read_gbq(query_jobs, project_id=project_id)

    query_logs = f"""
    SELECT ProcessID, MAX(LogTime) AS LastRun
    FROM `{project_id}.{logs_schema}.{table_log}`
    WHERE LOWER(Info) LIKE '%finished%'
    GROUP BY ProcessID
    """
    logs_df = pandas_gbq.read_gbq(query_logs, project_id=project_id)

    logs_dict = logs_df.set_index('ProcessID')['LastRun'].to_dict()
    jobs_to_trigger = []

    for _, row in jobs_df.iterrows():
        process_id = row['ProcessID']
        cron_expr = row['TriggerCron']
        process_name = row['ProcessName']

        try:
            cron = croniter(cron_expr, now)
            prev_scheduled = cron.get_prev(ret_type=datetime)
            prev_scheduled = prev_scheduled.replace(tzinfo=dt_timezone.utc)
        except Exception as e:
            log_message("WARNING", f"Invalid cron for ProcessID {process_id}: {cron_expr} – skipping.", context)
            continue

        last_run = logs_dict.get(process_id)

        if last_run is None or last_run < prev_scheduled:
            jobs_to_trigger.append({
                'ProcessID': process_id,
                'DagName': process_name,
                'RunID': generate_run_id(process_id, context=context)
            })

    log_message("INFO", f"✅ Jobs to trigger: {jobs_to_trigger}", context)
    ti.xcom_push(key='jobs_to_trigger', value=jobs_to_trigger)


def trigger_dags(**kwargs):
    """
    Triggers downstream DAGs using TriggerDagRunOperator.
    """
    ti = kwargs['ti']
    context = kwargs
    jobs = ti.xcom_pull(task_ids='get_jobs_to_trigger', key='jobs_to_trigger') or []

    for job in jobs:
        triggered_dag_id = job['DagName']
        run_id = job['RunID']
        process_id = job['ProcessID']
        try:
            trigger_dag_task = TriggerDagRunOperator(
                task_id=f"trigger_{triggered_dag_id}_{process_id}_{run_id}",
                trigger_dag_id=triggered_dag_id,
                wait_for_completion=False,
                conf={
                    "ProcessID": process_id,
                    "RunID": run_id
                }
            )
            trigger_dag_task.execute(context=kwargs)
            log_message("INFO", f"🚀 Triggered DAG '{triggered_dag_id}' (run_id={run_id})", context)
        except Exception as e:
            log_message("ERROR", f"❌ Failed to trigger DAG '{triggered_dag_id}' (run_id={run_id}): {e}", context)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1), 
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# Define the DAG
with DAG(
    dag_id= DAG_ID,
    description="This DAG responsible for the orchestration of all DAGs.",
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
        on_success_callback=on_task_success,
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
        on_success_callback=on_task_success,
        doc_md="""
        ### request_data_task
        """
    )  

    write_to_log_start_task = PythonOperator(
            task_id='write_to_log_start',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Master DAG Started'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### write_to_log_start_task
            """
    )

    write_to_log_end_task = PythonOperator(
            task_id='write_to_log_end',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Master DAG Finished'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### write_to_log_end_task
            """
    )

    get_jobs_to_trigger_task = PythonOperator(
        task_id='get_jobs_to_trigger',
        python_callable=get_jobs_to_trigger,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq,
        on_success_callback=on_task_success, 
        doc_md="""
        ### get_jobs_to_trigger_task
        """
    )

    trigger_dags_task = PythonOperator(
        task_id='trigger_dags',
        python_callable=trigger_dags,
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success,
        doc_md="""
        ### trigger_dags_task
        """
    )

    
    get_gcp_project_id_task >> get_system_params_task >> write_to_log_start_task >> get_jobs_to_trigger_task >> trigger_dags_task >> write_to_log_end_task