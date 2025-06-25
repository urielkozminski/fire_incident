- Technologies Used:

Google Cloud Composer (Airflow)
Dataform
BigQuery

- Overview of the DAGs:

There are four main DAGs in the architecture:

Master_DAG: Orchestrates the execution of all processes based on their configured cron schedules.

Import_DAG: Responsible for importing data from the Fire Incident endpoint into Google Cloud Storage. (I chose to include this step to preserve the raw data in a bucket for future auditing or reprocessing.)

Ingestion_DAG: Loads data from GCS into BigQuery.

Processing_DAG: Executes Dataform models using specific tags to determine which models to run.

- Architecture Design:
The main goal behind this architecture is to provide a generic, scalable, and flexible way to manage data imports and model executions. While the system is not yet fully completed due to time constraints, the foundation allows for easy extension.

Configuration Tables
JobsConfig: Contains configuration details for each process, such as cron expressions, Dataform tags, and custom properties.

Params: Stores global parameters used across DAGs. For security reasons, key values like schema names, bucket names, and repository identifiers are stored here.

- Modeling with Dataform:
Dataform, similar to dbt but native to the GCP ecosystem, was used for modeling.

I've created two example models:

fire_incident_with_year
Promotes the raw fire_incident table from the bronze layer to silver, while also extracting and adding the incident_year from the incident_date.

incident_count_by_year
Uses the fire_incident_with_year model to create an annual aggregation of incidents in the gold layer.

These models serve as examples, but if the BI team needs a specific dataset or transformation, they can create a new model in Dataform. As long as the model has the correct tag, it will be executed by the Processing_DAG. Execution is automated simply by registering the process in the JobsConfig table, enabling Master_DAG to pick it up.

- Future Improvements:
To enhance the robustness and traceability of the system, future improvements could include:

JobState and JobStateHistory tables to track execution status and history.

A Connection table to allow dynamic source and destination resolution via the Properties field in JobsConfig.


Uriel Kozminski