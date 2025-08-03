-- Database initialization script for containerized PostgreSQL
-- This file should be mounted to /docker-entrypoint-initdb.d/ in the postgres container

-- Create DEV database and schemas
CREATE DATABASE "DB_SR_DEV";

\c "DB_SR_DEV";

CREATE SCHEMA IF NOT EXISTS "STAGING";

-- Create PRD database and schemas
CREATE DATABASE "DB_SR_PRD";

\c "DB_SR_PRD";

CREATE SCHEMA IF NOT EXISTS "STAGING";

-- Create a dedicated user for the application (recommended for security)
-- Environment variables are defined in .env.local
CREATE USER :AIRFLOW_DB_USER WITH PASSWORD :'AIRFLOW_DB_PASSWORD';

-- Grant database connection permissions
GRANT CONNECT ON DATABASE "DB_SR_DEV" TO :AIRFLOW_DB_USER;
GRANT CONNECT ON DATABASE "DB_SR_PRD" TO :AIRFLOW_DB_USER;

-- Connect to DEV to set schema permissions
\c "DB_SR_DEV";
GRANT USAGE, CREATE ON SCHEMA "STAGING" TO :AIRFLOW_DB_USER;

-- Connect to PRD to set schema permissions
\c "DB_SR_PRD";
GRANT USAGE, CREATE ON SCHEMA "STAGING" TO :AIRFLOW_DB_USER;
