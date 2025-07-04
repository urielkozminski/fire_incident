DAGS EXPLANATION:


🔷 1. Master_DAG_fire
✅ Purpose
This DAG acts as a controller. It checks the JobsConfig table for active processes and their cron schedules, compares against logs in Log, and triggers other DAGs (Import_DAG_fire, Ingestion_DAG_fire, Processing_DAG_fire) only if needed.

🔁 Key Logic
Reads active jobs from JobsConfig

Checks last successful or failed runs from Log

Calculates if a process should run again using croniter

Triggers the appropriate DAGs with ProcessID and a generated RunID

📤 Expected Output Tables
Log: A centralized logging table that gets appended with entries like:

"Start" log when job begins

"Finished" or "Error" depending on task success/failure

Column	           Description
ProcessID	       ID of the job from JobsConfig
RunID	           Unique run identifier
Level	           Log level: Info, Error
Info	           Description of the event
LogTime	           Timestamp of the log
DataFormWorkflowID Populated only by Processing_DAG_fire

🔷 2. Import_DAG_fire
✅ Purpose
Fetches raw incident data from an external Fire Incident endpoint, and stores it in Google Cloud Storage (GCS).

🔁 Key Logic
Makes a request to the Fire Incident API

Saves the raw response as JSON (or similar format) to GCS

Logs start/success/error in BigQuery

📤 Expected Output
Raw data file in GCS: e.g., gs://<bucket>/fire_incidents/YYYY-MM-DD/*.json

Log entry in BigQuery indicating the process ran and succeeded

🔷 3. Ingestion_DAG_fire
✅ Purpose
Loads the raw data from GCS into BigQuery and performs any staging or transformation needed to populate structured tables.

🔁 Key Logic
Reads raw data files from GCS

Parses and ingests them into BigQuery

Likely creates/updates:

fire_incident_stg: a staging table

dim_time, dim_location, dim_response_time: dimension tables (snowflake schema)

📤 Expected Output Tables
Table	              Description
fire_incident_stg	  Raw/staged data from fire incidents (1 row per record)
dim_time              Time dimension (date, hour, etc.)
dim_location	      Location dimension (city, coordinates, etc.)
dim_response_time	  Time to respond, duration categorizations

These tables should be queried by downstream analytics or modeling tools.

🔷 4. Processing_DAG_fire
✅ Purpose
Executes Dataform workflows to model the ingested/staged data into clean, production-level datasets for analytics.

🔁 Key Logic
Checks if a workflow with the same tag is currently running

Retrieves the relevant repository and tags from BigQuery

Compiles the Dataform repo and creates a workflow invocation

Tracks workflow_invocation_id and logs it in BigQuery

📤 Expected Output Tables
These are the final analytical tables created by Dataform (within BigQuery):

Table Name (Example)	     Description
fire_incident_fact	         Fact table containing metrics like total incidents, response time, etc.
incident_summary	         Aggregated report per time/location
fire_incident_errors	     Errors or records that couldn’t be matched/processed

The actual list depends on your Dataform definitions. These are invoked via DataformCreateWorkflowInvocationOperator.

🧠 Summary Table
DAG	                    Role	                      Output
Master_DAG_fire	        Trigger controller	          Logs to Log table
Import_DAG_fire	        Get raw data → GCS	          JSON files in GCS + Log entries
Ingestion_DAG_fire	    GCS → BigQuery (staging/dim)  fire_incident_stg, dimension tables
Processing_DAG_fire	    BigQuery staging → Dataform	  Final modeled tables (via Dataform)