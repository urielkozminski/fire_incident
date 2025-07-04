import pytest
from unittest.mock import patch, MagicMock
from io import StringIO
import pandas as pd
from google.cloud import storage
import google.auth

import dags.Import_DAG_fire as import_dag # Replace with your actual module name


@pytest.fixture
def mock_context():
    """Creates a mock Airflow context with XCom and dag_run."""
    ti_mock = MagicMock()
    ti_mock.xcom_pull = MagicMock()

    dag_run_mock = MagicMock()
    dag_run_mock.conf.get.side_effect = lambda key: {'ProcessID': 123, 'RunID': 'run-xyz'}.get(key)

    return {
        'ti': ti_mock,
        'dag_run': dag_run_mock,
        'task_instance': ti_mock,
        'dag': MagicMock()
    }


def test_get_processid_runid(mock_context):
    """Test if get_processid_runid correctly sets values to XCom."""
    import_dag.get_processid_runid(**mock_context)
    mock_context['ti'].xcom_push.assert_called_with(
        key='get_processid_runid',
        value={'process_id': 20, 'run_id': 'aaa202020'}
    )


@patch("google.auth.default", return_value=(None, "mock-project"))
def test_get_gcp_project_id(mock_default, mock_context):
    """Test if GCP project ID is retrieved and pushed to XCom."""
    import_dag.get_gcp_project_id(**mock_context)
    mock_context['ti'].xcom_push.assert_called_with(key='project_id', value='mock-project')


@patch("pandas_gbq.read_gbq")
def test_get_system_params(mock_read_gbq, mock_context):
    """Test fetching and formatting system parameters from BQ."""
    mock_context['ti'].xcom_pull.side_effect = lambda task_ids, key=None: 'mock-project'
    df_mock = pd.DataFrame({
        'ParamID': [1, 2],
        'ParamName': ['bucket_name', 'gcs_path'],
        'ParamValue': ['test-bucket', 'some/path']
    })
    mock_read_gbq.return_value = df_mock

    result = import_dag.get_system_params(**mock_context)
    assert result == {'1': 'test-bucket', '2': 'some/path'}
    mock_context['ti'].xcom_push.assert_called_with(key='system_params', value=result)


@patch("requests.get")
def test_request_data_success(mock_get, mock_context):
    """Test successful API data fetch and XCom push."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
            {
                "incident_number": "23074761",
                "exposure_number": "0",
                "id": "230747610",
                "address": "PINE STREET",
                "incident_date": "2023-06-04T00:00:00.000",
                "call_number": "231551664",
                "alarm_dttm": "2023-06-04T14:16:36.000",
                "arrival_dttm": "2023-06-04T14:21:40.000",
                "close_dttm": "2023-06-04T14:23:25.000",
                "city": "San Francisco",
                "zipcode": "94108",
                "battalion": "B01",
                "station_area": "02",
                "box": "1325",
                "suppression_units": "1",
                "suppression_personnel": "5",
                "ems_units": "0",
                "ems_personnel": "0",
                "other_units": "0",
                "other_personnel": "0",
                "fire_fatalities": "0",
                "fire_injuries": "0",
                "civilian_fatalities": "0",
                "civilian_injuries": "0",
                "number_of_alarms": "1",
                "primary_situation": "500 Service Call, other",
                "mutual_aid": "N None",
                "action_taken_primary": "70 Assistance, other",
                "property_use": "962 Residential street, road or residential driveway",
                "no_flame_spread": "NA",
                "supervisor_district": "3",
                "neighborhood_district": "Nob Hill",
                "point": {
                "type": "Point",
                "coordinates": [-122.408978, 37.791088]
                },
                "data_as_of": "2023-06-04T14:23:25.000",
                "data_loaded_at": "2025-07-04T02:21:18.000"
            }
        ]
    mock_response.status_code = 200
    mock_response.raise_for_status = lambda: None
    mock_get.return_value = mock_response

    import_dag.request_data(**mock_context)
    mock_context['ti'].xcom_push.assert_called()


@patch("google.cloud.storage.Client")
def test_save_df_to_gcs(mock_storage_client, mock_context):
    """Test saving a DataFrame to a GCS bucket."""
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_context['ti'].xcom_pull.side_effect = lambda task_ids, key=None: {
        ('request_data', 'request_data'): df,
        ('get_system_params', 'system_params'): {'6': 'mock-bucket'}
    }.get((task_ids, key), df)

    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_client

    import_dag.save_df_to_gcs(**mock_context)
    mock_blob.upload_from_string.assert_called()