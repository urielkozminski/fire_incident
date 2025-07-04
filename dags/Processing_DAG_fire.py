from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
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
from google.auth import default
from googleapiclient import discovery
import os
import pytz
from google.cloud import storage
from io import StringIO
from googleapiclient.errors import HttpError
import google.auth
from airflow.exceptions import AirflowException
from Configs.config import enviroment_dataset, table_log, system_params, table_config

DAG_ID = 'Processing_DAG_fire'
REGION = 'me-west1'
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

    workflow_exec_id = ti.xcom_pull(key='workflow_invocation_id')

    system_params_dict = ti.xcom_pull(task_ids='get_system_params', key='system_params')
    logs_schema = system_params_dict['2']

    insert_log_query = f"""
    INSERT INTO `{project_id}.{logs_schema}.{table_log}` (ProcessID, RunID, Level, Info, LogTime, DataFormWorkflowID)
    VALUES ({process_id}, '{run_id}', '{log_level}', '{log_message}', CURRENT_TIMESTAMP(), { f"NULL" if workflow_exec_id is None else f"'{workflow_exec_id}'" })
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

        process_id = 40
        run_id = 'a04040aa'

        get_processid_runid = {"process_id": process_id, "run_id": run_id}
        log_message("INFO", f"Importing data with ProcessID: {process_id}, RunID: {run_id}", kwargs)
        kwargs['ti'].xcom_push(key='get_processid_runid', value=get_processid_runid)
        log_message("INFO", f"Pushing to XCom: {get_processid_runid}", kwargs)
    except Exception as e:
        log_message("ERROR", "Missing ProcessID or RunID.", kwargs)
        raise Exception("Missing ProcessID or RunID.")

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

def fetch_repo_id(**kwargs):

    """
    Update Job State, and Job State History table.
    
    Args:
    - kwargs: context variables provided by Airflow.

    Returns:
    dict: A dictionary containing 'ParamID' and 'ParamValue'.

    """
    ti = kwargs["ti"]
    context = kwargs
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')

    system_params_dict = ti.xcom_pull(task_ids='get_system_params')
    schema_processes = system_params_dict['1']
    schema_logs = system_params_dict['2']
    repo_id = system_params_dict['7']

    processid_runid = ti.xcom_pull(task_ids='get_processid_runid', key='get_processid_runid')
    process_id = processid_runid['process_id']

    query = f"""
    SELECT distinct
        jc.DataFormTags,
        lg.DataFormWorkflowID,
        MAX(lg.LogTime) AS LastRun
    FROM `{project_id}.{schema_processes}.{table_config}` as jc
    JOIN `{project_id}.{schema_logs}.{table_log}` as lg
        ON jc.ProcessID = lg.ProcessID
    WHERE jc.ProcessID = {process_id} AND LOWER(lg.Info) LIKE '%finished%' or LOWER(lg.Info) LIKE '%rror%'
    GROUP BY
        jc.DataFormTags, lg.DataFormWorkflowID;
    """

    log_message("INFO", f"Fetching tags and dataform workflow IDs from query: {query}", context)

    try:
        rows = pandas_gbq.read_gbq(query, project_id=project_id)
        log_message("INFO", f"Query results: {rows.to_string(index=False)}", context)
        if rows.empty:
            log_message("ERROR", "No record found.", context)
            raise Exception("No record found.")

        dataform_tags = rows.iloc[0]['DataFormTags']
        dataform_execution_id = rows.iloc[0]['DataFormWorkflowID']

        log_message("INFO", f'Repo ID: {repo_id}', context)
        log_message("INFO", f'Data Form Tags: {dataform_tags}', context)
        log_message("INFO", f'Data Form Execution ID: {dataform_execution_id}', context)
        repo_info = {"repo_id": repo_id, "dataform_tags": dataform_tags, "dataform_execution_id": dataform_execution_id}
        log_message("INFO", f'Repo Info: {repo_info}', context)
        kwargs['ti'].xcom_push(key='repo_info', value=repo_info)
        return repo_info

    except Exception as e:
        log_message("ERROR", f"Error fetching system params: {e}", context)
        raise Exception(f"Error fetching system params: {e}")

def check_if_dataform_job_running(**kwargs):
    """
    Checks if a Dataform job with specific tags is already running.

    This function:
    - Retrieves project ID, repository ID, and Dataform tags from Airflow XCom.
    - Lists all current workflow invocations in the Dataform repository.
    - Scans active invocations for a matching tag and RUNNING state.
    - Raises an Exception if a Dataform job with any of the specified tags is currently running.

    Args:
        kwargs (dict): Context provided by Airflow, including task instance (ti).

    Raises:
        Exception: If a Dataform job with matching tags is already in RUNNING state.
    """

    credentials, project_id = default()
    context = kwargs
    project_id = kwargs['ti'].xcom_pull(task_ids='get_gcp_project_id', key='project_id')
    log_message("INFO", f"Checking running Dataform jobs in project: {project_id}", context)

    fetch_repo_id = kwargs['ti'].xcom_pull(task_ids='fetch_repo_id', key='repo_info')
    repository_id = fetch_repo_id['repo_id']
    log_message("INFO", f"Repository ID: {repository_id}", context)
    dataform_tags = fetch_repo_id['dataform_tags']
    log_message("INFO", f"Checking for active jobs with any of these tags: {dataform_tags}", context)
    dataform_execution_id = fetch_repo_id['dataform_execution_id']
    if dataform_execution_id is None:
        log_message("INFO", f"data form execution id is not found for tag {dataform_tags}", context)
        return
    log_message("INFO", f"Dataform tags: {dataform_tags}", context)

    service = discovery.build('dataform', 'v1beta1', credentials=credentials)

    name = f"projects/{project_id}/locations/{REGION}/repositories/{repository_id}/workflowInvocations/{dataform_execution_id}"
    request = service.projects().locations().repositories().workflowInvocations().get(name=name)
    try:
        response = request.execute()
        state = response.get('state')
        if state in ('RUNNING'):
            log_message("ERROR", f"{dataform_tags} is already running", context)
            raise Exception("Dataform already running")
        else:
            log_message("INFO", f"{dataform_tags} is not running", context)
    except Exception as e:
        log_message("ERROR", f"Error retrieving workflow invocation {dataform_tags}: {e}", context)


def create_compilation_result(**kwargs):

    """
    Created complication results which will be used for running actual dataform definitions.
    
    Args:
    - kwargs: context variables provided by Airflow.

    Returns:
    dict: A dictionary containing compilation results.

    """

    ti = kwargs['ti']
    context = kwargs
    project_id = ti.xcom_pull(task_ids='get_gcp_project_id', key='project_id')
    repository_branch = 'main'
    log_message("INFO", f"Dataform default branch: {repository_branch}", context)
    fetch_repo_id = ti.xcom_pull(task_ids='fetch_repo_id')
    repository_id = fetch_repo_id['repo_id']
    log_message("INFO", f"repository id: {repository_id}", context)
    log_message("INFO", "Compilation Results starting...", context)
    try:
        compilationOp = DataformCreateCompilationResultOperator(
            task_id="create_compilation_result_inner",
            project_id=project_id,
            region=REGION,
            repository_id= repository_id,
            compilation_result={
                "git_commitish": repository_branch
            },
        )

        result = compilationOp.execute(context=kwargs)
        log_message("INFO", "Compilation Results obtained...", context)
        ti.xcom_push(key='compilation_result_name', value=result['name'])
    except HttpError as http_err:
        log_message("ERROR", f"HTTP error during compilation: {http_err}", context)
        raise Exception(f"HTTP error during compilation: {http_err}")
    except AirflowException as ae:
        log_message("ERROR", f"Airflow operator failure during compilation: {ae}", context)
        raise Exception(f"Airflow error during compilation: {ae}")
    except Exception as e:
        log_message("ERROR", f"Unexpected error in create_compilation_result(): {e}", context)
        raise Exception(f"Unexpected error in create_compilation_result(): {e}")

def create_workflow_invocation(**kwargs):

    """
    Runs data form workflows.
    
    Args:
    - kwargs: context variables provided by Airflow.
    
    """
    ti = kwargs['ti']
    context = kwargs
    project_id = kwargs['ti'].xcom_pull(task_ids='get_gcp_project_id', key='project_id')
    fetch_repo_id = ti.xcom_pull(task_ids='fetch_repo_id')
    repository_id = fetch_repo_id['repo_id']
    log_message("INFO", f"repository id: {repository_id}", context)
    dataform_tags = fetch_repo_id['dataform_tags']
    log_message("INFO", f"dataform_tags: {dataform_tags}", context)
    compilation_result = kwargs['ti'].xcom_pull(key='compilation_result_name', task_ids='create_compilation_result')

    log_message("INFO", "Dataform workflow Invocation starting...", context)
    try:
        InvocationOp = DataformCreateWorkflowInvocationOperator(
            task_id="create_workflow_invocation",
            project_id= project_id,
            region=REGION, 
            repository_id=repository_id,
            workflow_invocation={
                "compilation_result": compilation_result,
                "invocation_config": {"included_tags": [dataform_tags], "transitive_dependencies_included": True }
            },
            asynchronous = True
        )

        result = InvocationOp.execute(context=kwargs)
        invocation_name = result.get('name')
        log_message("INFO", f"Dataform workflow invoked with name: {invocation_name}", context)

        workflow_invocation_id = invocation_name.split('/')[-1]
        log_message("INFO", f"Workflow Invocation ID: {workflow_invocation_id}", context)

        kwargs['ti'].xcom_push(key='workflow_invocation_id', value=workflow_invocation_id)

        log_message("INFO", "Dataform workflow Invocated...", context)
    except HttpError as http_err:
        log_message("ERROR", f"HTTP error during workflow invocation: {http_err}", context)
        raise Exception(f"HTTP error during workflow invocation: {http_err}")
    except AirflowException as ae:
        log_message("ERROR", f"Airflow operator failure during workflow invocation: {ae}", context)
        raise Exception(f"Airflow error during workflow invocation: {ae}")
    except Exception as e:
        log_message("ERROR", f"Unexpected error in create_workflow_invocation(): {e}", context)
        raise Exception(f"Unexpected error in create_workflow_invocation(): {e}")
        
    
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
            on_success_callback=on_task_success,
            doc_md="""
            ### get_gcp_project_id_task
            """
    )

    write_to_log_start_task = PythonOperator(
            task_id='write_to_log_start',
            python_callable=write_to_log,
            op_kwargs={
            'log_level': 'Info',
            'log_message': 'Processing DAG Started'
        },
            provide_context=True,
            on_failure_callback = insert_failure_log_into_bq,
            on_retry_callback = insert_failure_log_into_bq, 
            on_success_callback=on_task_success,
            doc_md="""
            ### get_gcp_project_id_task
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

    fetch_repo_id_task = PythonOperator(
        task_id='fetch_repo_id',
        python_callable=fetch_repo_id,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success,
        provide_context=True,
        doc_md="""
        ### Get Proces ID and Run ID Task
        This task recieves the process id and run id from the master dag and returns it.
        """
    )


    create_compilation_result_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq,
        on_success_callback=on_task_success, 
        provide_context=True,
    )

    create_workflow_invocation_task = PythonOperator(
        task_id='create_workflow_invocation',
        python_callable=create_workflow_invocation,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success,
        provide_context=True,
    )

    check_if_dataform_job_running_task = PythonOperator(
        task_id='check_if_dataform_job_running',
        python_callable=check_if_dataform_job_running,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success, 
        provide_context=True,
        doc_md="""
        ### Get Proces ID and Run ID Task
        This task recieves the process id and run id from the master dag and returns it.
        """
    )

    write_to_log_end_task = PythonOperator(
        task_id='write_to_log_end',
        python_callable=write_to_log,
        op_kwargs={
        'log_level': 'Info',
        'log_message': 'Processing DAG Finished'
        },
        provide_context=True,
        on_failure_callback = insert_failure_log_into_bq,
        on_retry_callback = insert_failure_log_into_bq, 
        on_success_callback=on_task_success,
        doc_md="""
        ### write_to_log_end
        """
    )

    get_gcp_project_id_task >> get_processid_runid_task >> get_system_params_task >> write_to_log_start_task >> fetch_repo_id_task >> check_if_dataform_job_running_task  
    check_if_dataform_job_running_task >> create_compilation_result_task >> create_workflow_invocation_task >> write_to_log_end_task