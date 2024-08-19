# LeanerQuery dbt Package

This is a dbt package built to help teams that use BigQuery understand their costs associated with dbt and general use.

This package uses [BigQuery Audit](https://cloud.google.com/logging/docs/reference/audit/bigquery/rest/Shared.Types/AuditData) Log data and assumes that a log sink is set up to export the logs into tables in a BigQuery dataset.  If you are unfamiliar with how to accomplish this, visit this Google cloud [resource](https://cloud.google.com/blog/products/data-analytics/bigquery-audit-logs-pipelines-analysis).

This package assumes that the user(s) executing the processes have read access to the BigQuery log dataset referenced above and write access to the dataset where LeanerQuery is creating/updating objects.

The package contains a lot of variable values to determine costs, aggregation, and scoring.  You will want to override/specify some of these values in your `dbt_project.yml` file as your details and use cases are undoubtedly different than ours.  More details in the [variables](#variables) section.

This dbt package aims to provide the following details for data teams who are using BigQuery:
- costs associated with queries and dbt builds
- errors, with codes and messages that your users are getting from BigQuery
- easy categorization and classification of your dbt models through a scoring system that assigns an **importance**, **threat**, and **overall priority** score per model.

## Quick Links
- **[Getting Started](#getting-started)**
- **[Models](#models)**
- **[Reports](#reports)**
- **[Scoring logic](#scoring-logic)**
- **[Variables](#variables)**
- **[Visualization](#visualization)**

## Getting Started
- Add any/all variable overrides to your `dbt_project.yml` file, e.g.:
```YML
leaner_query_database: my_gcp_project
leaner_query_importance_query_score_3: ['My BI Tool']
leaner_query_importance_query_score_4: ['My reverse ETL tool']

leaner_query_prod_dataset_names: ['marts','reports']
leaner_query_stage_dataset_names: ['staging_models']
leaner_query_database: ['generic-bigquery-project', 'other-generic-bigquery-project']
leaner_query_source_schema: ['audit_dataset']
leaner_query_data_access_table: ['audit_table']

leaner_query_custom_clients: [
{'user_agent': 'agent_string', 'principal_email':'username', 'client_name':'Custom Client 1'},
{'user_agent': 'agent_string', 'principal_email':'different_username', 'client_name':'Custom Client 2'},
]
  
leaner_query_custom_egress_emails: [
'egress_sa@your-project.iam.gserviceaccount.com',
'another_sa@your-project.iam.gserviceaccount.com',
]
```
- Run `leaner_query_setup.py`, located at the root directory of the package. This populates `./dbt_packages/leaner_query/models/staging/bigquery_audit_log/src_bigquery_audit_log.yml` with values from the `leaner_query_database`, `leaner_query_source_schema`, and `leaner_query_data_access_table` variables that you've input. The script assumes a typical dbt installation, and looks for `dbt_project.yml` at root of your dbt repo.
    - **If you have added any of the `leaner_query_database`, `leaner_query_source_schema`, and `leaner_query_data_access_table` variables, even with just one project, this step MUST be completed! Please ensure any values in these variables are in a list (`[]`). This change was introduced in version 0.2.0.**
    - If you haven't added `leaner_query_database`, `leaner_query_source_schema`, and `leaner_query_data_access_table` as variables and you're okay with using default values, you do **not** have to run this step.
    - If you modify `models/staging/bigquery_aduit_log/stg_bigquery_audit_log__data_access.sql`, please copy/paste the text from the `source` CTE to the end of the file into `stg_bigquery_audit_log__data_access_query_text.txt`. This text file is used in `leaner_query_setup.py.`

- Optionally update your dbt_project.yml file to override the dataset where the leaner_query models will be built (defaults to `leaner_query`):
```YML
leaner_query:
    +schema: leaner_query_output
```
- Run via tag:
```
	dbt run -s tag:leaner_query
```

### Models
The package outputs a dimensional model that allows users to build upon for custom analysis and reporting.  This dimensional model contains:
- **dim_bigquery_users**: all users who have executed a statement against BQ.  Uses the `principal_email` attribute to classify users as a **user** or a **service account**.
- **dim_error_messages**: all distinct error messages that have been issued by BQ.
- **dim_job_labels**: bridge-type table that contains label keys and values for jobs
- **dim_job_table_view_references**: combination of all tables and views ever referenced in the BQ audit logs along with which layer they are a part of.  The layer classification uses the values set in the  `leaner_query_prod_dataset_names` and `leaner_query_stage_dataset_names` variables. 
- **dim_jobs**: details about every BQ job executed, including parsed and normalized dbt metadata that is sent to BQ.
- **dim_query_statements**: essentially just `query_statements` and `dbt_info` for each given BQ job. This was originally all part of `dim_jobs`, but was broken out as it's not used often and accounsted for a majority of the size of the table.
- **dim_user_agents**: parsed and classified caller_supplied_user_agent details, defined as a `client_type`.  The classification logic can be seen below and is augmented by values added to the `leaner_query_custom_clients` variable.
- **fct_executed_statements**: BQ job execution event data including output rows, slot_ms, processed_bytes, billed_bytes, etc.
#### Client type classification logic:
- **Connected Sheet - User Initiated**: the job contained a label of 'sheets_trigger' with a value of 'user'
- **Connected Sheet - Scheduled**: the job contained a label of 'sheets_trigger' with a value of 'schedule'
- **Scheduled Query**: the job contained a label of 'data_source_id' with a value of 'scheduled_query'
- **dbt run**: the job contained a label of 'dbt_invocation_id' with any value or the user_agent contains 'dbt'
- **Web console**: the job's user_agent contains 'Mozilla'
- **Python Client**: the job's user_agent contains 'gl-python'
- **Fivetran**: the job's user_agent contains 'Fivetran'
- **Hightouch**: the job's user_agent contains 'Hightouch'
- **Rudderstack**: the job's user_agent contains 'gcloud-golang-bigquery' and the principal_email contains 'rudderstack' 
- **Golang Client**: the job's user_agent contains 'gcloud-golang' or the user_agent contains 'google-api-go'
- **Node Client**: the job's user_agent contains 'gcloud-node'
- **Java Client**: the job's user_agent contains 'SimbaJDBCDriver'
- **Stemma Crawler**: the job's user_agent contains '(gzip),gzip(gfe)'

### Reports
**Note**: Reports can be disabled by changing the `leaner_query_enable_reports` variable value to false.
- **rpt_bigquery_dbt_metrics_daily**:  produces aggregates, on a daily and dbt model grain, including:
  - total_dbt_builds
  - total_dbt_tests
  - total_dbt_snapshots
  - total_estimated_dbt_run_cost_usd
  - total_estimated_dbt_test_cost_usd
  - total_estimated_snapshot_cost_usd
  - total_estimated_dbt_build_time_ms
  - total_estimated_dbt_test_time_ms
  - total_estimated_dbt_snapshot_time_ms
  - average_build_cost
  - average_test_cost
  - average_snapshot_cost
  - average_build_time_ms
  - average_test_time_ms
  - average_snapshot_time_ms
- **rpt_bigquery_table_usage_daily**: produces aggregates, on a daily, table/view, layer (raw, stage, prod), and client type grain:
  - total_queries_run
  - dbt_models_run
  - dbt_tests_run
  - total_human_users
  - total_service_accounts
  - total_errors
  - threat_score
  - importance_score
  - priority_score
- **rpt_bigquery_usage_cost_daily**: produces cost specific aggregates, on a daily and client type grain:
  - total_queries_run
  - total_estimated_cost_usd (*includes queries and other statement types*)
  - total_estimated_dbt_run_build_cost_usd
  - total_estimated_dbt_run_test_cost_usd
  - total_time_ms (*includes queries and other statement types*)
  - total_estimated_query_cost_usd (*only includes queries*)
  - total_query_time_ms (*only includes queries*)
- **rpt_bigquery_user_metrics_daily**: produces aggregates, on a daily, user, and user_type (service-account vs user) grain:
  - total_queries_run
  - total_errors
  - total_prod_tables_used
  - total_stage_tables_used
  - total_raw_tables_used
  - total_tables_used
  - total_estimated_cost_usd
  - total_time_ms
  - error_rate (total_errors/total_queries)
  - prod_table_use_rate (total_prod_tables_used/total_tables_used)
  - stage_table_use_rate (total_stage_tables_used/total_tables_used)
  - raw_table_use_rate (total_raw_tables_used/total_tables_used)
### Scoring logic
#### Importance scoring
We sought to evaluate how important each BQ object was by looking at them from four separate dimensions, which combine to total possible score of 100:
- **Service account usage** - if tables are being accessed (in query statements) by service accounts, the object is important enough to be part of an automated process.  We determine a 7-day total query count percentile rank (per table) and use the weight from the `leaner_query_weight_importance__service_account_queries` to calculate a service account usage component.
- **dbt usage** - if tables are being queried in our dbt build process, the object holds some level of importance because it is an obvious dependency for other models/tables.  We determine a 7-day toal query count percentile rank (per table) and use the weight from the `leaner_query_weight_importance__dbt_queries` to calculate a dbt usage component.
- **Egress usage** - tables that are used by egress processes are obviously important because we are likely impacting other systems within our organization.  We further classify egress in this score on a scale of 1-4 (least to most important) with the use of the values in the `leaner_query_importance_query_score_1...4`  variables. We then determine a 7-day query count percentile rank (per table)  and use the weight from the `leaner_query_weight_importance__egress_use` to calculate an egress usage component.
- **User breadth** - if tables are being accessed (in query statements) by a wide array of users (non-service account users), the object is important in ad hoc query and discovery processes.  We use a trailing 30 day active user metric (by table) and determine a 7-day query count percentile rank (by table) and use the weight from the `leaner_query_weight_importance__user_breadth` to calculate a user breadth component.
#### Threat scoring
We sought to evaluate how much of a threat or risk each BQ object was by looking at them from four separate dimensions, which combine to total possible score of 100:
- **Egress usage** - if tables are being used (by service accounts) to send data to other systems, it represents a possible threat or risk to our business.  In addition, we assert that egress use of tables that are in a non production data layer (determined by the `leaner_query_prod_dataset_names` variable) is very risky and is assessed a multiplier value (5 * the 7 day total query count).  We determine a 7-day total query count percentile rank (with any multiplier applied, per table) and use the weight from the `leaner_query_weight_threat__service_account_egress` to calculate an egress component.
- **Cost to query** - tables that are expensive to query are a threat or a risk to the business from a cost perspective and should be modeled and/or tuned to be more efficient.  We determine a 7-day total query cost percentile rank (per table) and use the weight from the `leaner_query_weight_threat__cost_to_query` to calculate a cost to query component.
- **Cost to build** - tables that are expensive to build (by dbt processes), again, are a threat or a risk because they could be unnecessarily costing our business money. We determine a 7-day total cost to build percentile rank (per table) and use the weight from the `leaner_query_weight_threat__cost_to_build` to calculate a cost to build component.
- **Daily errors** - if tables are being used incorrectly and/or are often the source of errors, they represent a risk to our reputation and overall usefulness to the organization.  We determine a 7-day total error percentile rank (per table) and use the weight from the `leaner_query_weight_threat__daily_errors` to calculate a daily error component.
#### Priority Score
Priorizing where to spend precious refactoring and refinement time is difficult and rarely data-backed.  To help prioritize these efforts, we combine the table importance score and the threat level score to develop an overall priority score.  The calculation is as follows:
```
	(importance score * importance_level_weight) + (threat score * threat_level_weight)
``` 
## Variables
### General purpose
- **leaner_query_database** 
	- **Description**: database (project) where the bigquery audit logs reside. Multiple projects may be used: input as a list.
	- **Default**: target.database
- **leaner_query_source_schema** 
	- **Description**: schema (dataset) where the bigquery audit logs reside. Input as a list, in the same order as projects you've inputted. `leaner_query_setup.py` will return an error if you have more values in this list than in `leaner_query_database`. If all of your values match the default value below, you do not need this variable.
	- **Default**: bigquery_audit_logs
- **leaner_query_data_access_table** 
	- **Description**: table name where the bigquery data access audit logs reside. Input as a list, in the same order as projects you've inputted. `leaner_query_setup.py` will return an error if you have more values in this list than in `leaner_query_database`. If all of your values match the default value below, you do not need this variable.
	- **Default**: leaner_query_data_access_table
- **leaner_query_enable_reports** 
	- **Description**: enable report models listed above.
	- **Default**: true
- **leaner_query_require_partition_by_reports** 
	- **Description**: enable requiring the use of partitions when querying report tables.
	- **Default**: true
- **leaner_query_prod_dataset_names** 
	- **Description**: a list of dataset names that are considered production (e.g. marts and reporting tables), meant for consumption by users and other systems.
	- **Default**: [] (None)
- **leaner_query_stage_dataset_names** 
	- **Description**: a list of dataset names that are considered staging and are not meant for consumption by users and other systems; used by dbt to build production models.
	- **Default**: [] (None)
- **leaner_query_bq_on_demand_pricing** 
	- **Description**: list price for BQ on-demand pricing per TB bytes billed.  You can adjust this if your contract has a differnt rate than the list price.
	- **Default**: 6.225
- **leaner_query_bq_slot_pricing** 
	- **Description**: list price for BQ slot based pricing per ms, rounded up to the minute.  You can adjust this if your contract has a differnt rate than the list price.
	- **Default**: 
	```json 
    {"standard": 0.04, "enterprise": 0.06, "enterprise_plus": 0.10}
    ```
- **leaner_query_bq_pricing_schedule** 
	- **Description**: your current pricing schedule.  Should be one of ('on_demand', 'standard', 'enterprise', 'enterprise_plus').
	- **Default**: on_demand
- **leaner_query_custom_clients** 
	- **Description**: a list of custom clients that extends the standard client list above.  
	- **Default**:[] (None)
```json
{
    "user_agent": "sample_user_agent_value",
	"principal_email": "optional_address", 
	"client_name": "sample_custom_client_name"
}
```
- **leaner_query_enable_dev_limits**
	- **Description**: This is used to limit the incremental builds in a dev environment so you aren't doing a full refresh during development and CI.
	- **Default**: true
- **leaner_query_dev_limit_days**
	- **Description**: Used in conjunction with `leaner_query_enable_dev_limits`, this determines how many days back you want to have your incremental models build in dev.
	- **Default**: 30
- **leaner_query_dev_target_name**
	- **Description**: The name of your target dev environment.
	- **Default**: "dev"
  ### Reporting
- **leaner_query_priority_threat_level_weight** 
	- **Description**: weight given to the threat report score
	- **Default**: 0.65
- **leaner_query_priority_importance_level_weight** 
	- **Description**: weight given to the threat report score
	- **Default**: 0.35
- **Threat report**:
	- **leaner_query_weight_threat__service_account_egress** 
		- **Description**: weight given to service account egress activity for an object
		- **Default**: 0.35
	- **leaner_query_weight_threat__cost_to_query** 
		- **Description**: weight given to the cost to query an object
		- **Default**: 0.30
	- **leaner_query_weight_threat__cost_to_build** 
		- **Description**: weight given to the cost to build an object (with dbt)
		- **Default**: 0.25
	- **leaner_query_weight_threat__daily_errors** 
		- **Description**: weight given to the volume of daily errors associated with an object
		- **Default**: 0.10
- **Importance report**:
	- 	- **leaner_query_importance_query_score_1** 
		- **Description**: list of client names to use to score the lowest importance queries
		- **Default**: ["Web console"]
	- 	- **leaner_query_importance_query_score_2** 
		- **Description**: list of client names to use to score the second lowest importance queries
		- **Default**: ["Connected Sheet - User Initiated", "Connected Sheet - Scheduled", "Scheduled Query"]
	- 	- **leaner_query_importance_query_score_3** 
		- **Description**: list of client names to use to score the second highest importance queries
		- **Default**: [] (None)
	- 	- **leaner_query_importance_query_score_4** 
		- **Description**: list of client names to use to score the highest importance queries
		- **Default**: [] (None)
	- **leaner_query_weight_importance__service_account_queries** 
		- **Description**: weight given to the volume of queries by service accounts for an object
		- **Default**: 0.20
	- **leaner_query_weight_importance__dbt_queries** 
		- **Description**: weight given to the cost to build an object with dbt
		- **Default**: 0.20
	- **leaner_query_weight_importance__egress_use** 
		- **Description**: weight given to volume of queries used by service accounts that perform egress for an object
		- **Default**: 0.35
	- **leaner_query_weight_importance__user_breadth** 
		- **Description**: weight given to the breadth of users querying an object
		- **Default**: 0.25

### Visualization
#### Grafana Dashboarding Template
We have included a template for our Grafana Dashboard that help to track our BQ and dbt costs. You can find the raw JSON in `grafana_dashboard_template.json`. You can easily import this dashboard into any Grafana instance and immediately start visualizing your data - assuming you have the BigQuery Datasource configured.