TEST_INGESTION.PY


- test_get_processid_runid
    Purpose: Check if the DAG run context is parsed correctly.

    Mocks: dag_run.conf.get

    What It Validates:

    ProcessID and RunID are extracted from dag_run.

    Result is pushed to XCom using key get_processid_runid.

- test_get_gcp_project_id
    Purpose: Validate retrieval of current GCP project ID from default credentials.

    Mocks: google.auth.default

    Expected Behavior:

    Pushes the returned project ID to XCom as project_id.

- test_get_system_params
    Purpose: Test pulling config values from System_Params table using pandas_gbq.read_gbq.

    Mocks: pandas_gbq.read_gbq

    Validation:

    Transforms DataFrame to a dict like {"1": "test-bucket", "2": "some/path"}.

    Pushes result to XCom as system_params.

- test_request_data_success
    Purpose: Ensure API fetch and JSON-to-DataFrame logic works as expected.

    Mocks: requests.get

    Validations:

    Response returns a mocked JSON payload.

    Data is converted to DataFrame and pushed to XCom as request_data.

- test_save_df_to_gcs
    Purpose: Validate that a DataFrame pulled from XCom is correctly uploaded to a GCS bucket.

    Mocks: google.cloud.storage.Client, bucket, blob

    Checks:

    Calls .to_csv() on the DataFrame.

    Ensures .upload_from_string() is called on the GCS blob object.