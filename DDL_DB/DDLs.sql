--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- SCHEMA BRONZE -- 

CREATE SCHEMA IF NOT EXISTS `fire_inc_bronze`
OPTIONS(location = 'me-west1');

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- SCHEMA SILVER -- 

CREATE SCHEMA IF NOT EXISTS `fire_inc_silver`
OPTIONS(location = 'me-west1');

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- SCHEMA GOLDEN -- 

CREATE SCHEMA IF NOT EXISTS `fire_inc_gold`
OPTIONS(location = 'me-west1');

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- SCHEMA AND TABLE ENVIROMENT --

CREATE SCHEMA IF NOT EXISTS `fire_inc_env`
OPTIONS(location = 'me-west1');

CREATE OR REPLACE TABLE `fire_inc_env.SystemParams` (
  ParamID INTEGER,
  ParamName STRING,
  ParamValue STRING,
  UpdateBy STRING,
  UpdateDate TIMESTAMP
);

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- SCHEMA AND TABLE LOGS --

CREATE SCHEMA IF NOT EXISTS `fire_inc_logs`
OPTIONS(location = 'me-west1');

CREATE OR REPLACE TABLE `fire_inc_logs.Logs` (
  ProcessID INTEGER,
  RunID STRING,
  Level STRING,
  Info STRING,
  LogTime TIMESTAMP,
  DataFormWorkflowID STRING
);

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- SCHEMA AND TABLA PROCESS --

CREATE SCHEMA IF NOT EXISTS `fire_inc_processes`
OPTIONS(location = 'me-west1');

CREATE OR REPLACE TABLE `fire_inc_processes.JobsConfig` (
  ProcessID INTEGER,
  ProcessName STRING,
  Description STRING,
  TriggerCron STRING,
  ProcType STRING,
  Properties JSON,
  IsActive BOOL,
  UpdateBy STRING,
  UpdateDate TIMESTAMP,
  DataFormTags STRING,
);
