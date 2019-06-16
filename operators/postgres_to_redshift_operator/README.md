# postgres_to_redshift_operator

This operator handles replication of data from Postgres database to a Redshift cluster.

Features:

1. support full replication / pre-replication transformation from the source,
    useful to handle annoyances such as maximum characters, special characters 
    etc. encountered during data migration.
    
2. support multiple tables replication

3. (Upcoming) support argument for sortkey and distkey, so the operator could infer
    the destination DDL from the source DDL, saving the trouble to pre-define schemas 
    in the destination database
    
4. (Upcoming) support truncate safety check at destination, if things went wrong allow rollback

 
# Requirements and Setup

1. setup a S3 bucket on AWS
2. create a local cache folder to store CSV files
3. create an IAM role that allows copying data from S3 to Redshift

Fill the plugin level constants at the start of the file.

```
AIRFLOW_CACHE_PATH = ''
AIRFLOW_CACHE_BUCKET = ''
AIRFLOW_CACHE_FOLDER = 'psql_copy_cache'
REDSHIFT_IAM_ROLE = ''
```

# Argument

We only list down additional Task Instance arguments here. 

| Argument Name      | Type           | Description |
| ------------- |:-------------:| -----:|
| source_conn_id | str | postgres connection ID |
| destination_hook_id | str | redshift connection ID |
| table_mapping | dict | A mapping of tables needed to be replicated, key is the source table, value is the destination |
| truncate | boolean | whether truncate the destination table, if `False`, data will append |

# Example

```
replicate_data = PostgresToRedshiftOperator(
    task_id='replicate_data',
    source_conn_id='example-source-conn-id',
    dest_conn_id='example-dest-conn-id',
    table_mapping={
        "public.chat_users": {
            "table_name": "raw_chats.chat_users",
        },
        "public.chat_messsages": {
            "table_name": "raw_chats.chat_messages",
            "transformation_sql": f"<absolute_path_to_sql>"
        }
    },
    truncate=True,
    dag=dag
```
