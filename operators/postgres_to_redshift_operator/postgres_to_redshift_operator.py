# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import subprocess

import boto3
import psycopg2
from jinja2 import Template
from sqlalchemy.engine.url import make_url

from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException

logger = logging.getLogger(__name__)

AIRFLOW_CACHE_PATH = ''
AIRFLOW_CACHE_BUCKET = ''
AIRFLOW_CACHE_FOLDER = 'psql_copy_cache'
REDSHIFT_IAM_ROLE = ''


def run_commands(statement: str, error_message: str = "") -> None:
    p = subprocess.Popen(statement, shell=True, stderr=subprocess.PIPE)
    _, error = p.communicate()
    if error:
        raise AirflowException(error.decode('utf-8') + error_message)


class PostgresToRedshiftOperator(BaseOperator):
    """
    Load data from Postgres tables into Redshift.

    PSQL COPY the tables into Redshift, via S3 bucket.

    It supports full replication and partial replication using a SELECT query as incremental source.

    """

    s3_client = boto3.client('s3')

    template_fields = ('table_mapping',)

    @apply_defaults
    def __init__(
            self,
            source_conn_id: str,
            dest_conn_id: str,
            table_mapping: dict,
            truncate: bool = False,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_hook = PostgresHook(source_conn_id)
        self.dest_hook = PostgresHook(dest_conn_id)
        self.table_mapping = table_mapping
        self.truncate = truncate
        self.context = None

    def execute(self, context):
        self.context = context

        for source_table, dest_table in self.table_mapping.items():
            # parse dest_table
            dest_table_name = dest_table.get("table_name")
            transformation_sql = dest_table.get('transformation_sql')

            # declare path
            local_file_path = os.path.join(
                AIRFLOW_CACHE_PATH, AIRFLOW_CACHE_FOLDER, f'{dest_table_name.replace(".", "_")}.csv'
            )
            s3_key = os.path.join(AIRFLOW_CACHE_FOLDER, f'{dest_table_name.replace(".", "_")}.csv')
            s3_full_path = f"s3://{AIRFLOW_CACHE_BUCKET}/{s3_key}"

            # dump data to local host
            self.dump_from_postgres(
                table=source_table,
                hook=self.source_hook,
                file_path=local_file_path,
                transformation_sql_path=transformation_sql  # dump data while applying transformation
            )

            logger.warning(f"Table {source_table} dumped to local file {local_file_path}")

            # upload to S3
            self.s3_client.upload_file(local_file_path, AIRFLOW_CACHE_BUCKET, s3_key)

            logger.warning(f"Local file {local_file_path} uploaded to {s3_full_path}")

            self.dump_from_s3_to_redshift(local_file_path, s3_key, s3_full_path, dest_table_name)

    def dump_from_s3_to_redshift(self, local_file_path, s3_key, s3_full_path, dest_table) -> None:
        """
        dump data from s3 to redshift
        :param local_file_path: absolute local path of dumped data
        :param s3_key: s3 key
        :param s3_full_path: s3 path
        :param dest_table: fully qualified destination table
        :return: None
        """
        logger.warning(f"Loading table {dest_table} from {s3_full_path}")
        truncate_clause = ''

        if self.truncate:
            truncate_clause = f"TRUNCATE TABLE {dest_table};"

        copy_to_redshift_sql = f"""
                BEGIN;

                {truncate_clause}

                COPY {dest_table}
                FROM 's3://{AIRFLOW_CACHE_BUCKET}/{s3_key}'
                IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                DELIMITER '|' 
                IGNOREHEADER 1
                ESCAPE
                REMOVEQUOTES
                ;

                COMMIT;
            """

        try:
            self.dest_hook.run(copy_to_redshift_sql)
        except psycopg2.InternalError as e:
            if "stl_load_errors" in str(e):
                logger.warning("Loading error, checking reason in Redshift.")
                # check err reason in redshift loading
                check_sql = f"""
                        SELECT err_reason FROM stl_load_errors
                        WHERE filename = '{s3_full_path}'
                        ORDER BY starttime DESC
                        LIMIT 1;
                    """
                error_reason = self.dest_hook.get_records(check_sql)[0][0].strip()
                logger.warning(f"Error reason: {error_reason}")

        self.dest_hook.run(copy_to_redshift_sql)

        # remove local file afterwards
        os.remove(local_file_path)

    def dump_from_postgres(self, table, hook, file_path: str, transformation_sql_path: str = None) -> None:
        """
        dump data from postgres to local path
        :param table: fully qualified table name
        :param hook: hook used
        :param file_path: local file path
        :param transformation_sql_path: transformation SQL applied to source data
        :return:
        """
        if transformation_sql_path is None:
            sql = f"SELECT * FROM {table}"

            if not self.truncate:
                raise AirflowException("The destination is not set to truncated, "
                                       "while copying the entire source table.")
        else:
            sql = self.read_sql_from_file(transformation_sql_path)

        dburi = make_url(hook.get_uri())
        stmt = f"""PGPASSWORD='{dburi.password}' psql -X -U {dburi.username} -h {dburi.host} \
            -d {dburi.database} -c "\copy (

                {sql}

            ) TO '{file_path}' DELIMITER '|' \
            CSV HEADER;" """
        run_commands(stmt)

    def read_sql_from_file(self, file_path: str) -> str:
        """
            read SQL file from input directory, used by operator for
            incremental replication or pre-replication transformation

        :param file_path: absolute path of the SQL
        :return: (templated if applicable) SQL
        """

        with open(file_path, 'r') as sql_file:
            sql_str = sql_file.read()

            # remove semi-column in PSQL copy statement just in case
            final_sql = Template(sql_str).render(self.context).replace(";", "")

            logger.info(f"Psql Copy SQL: {final_sql}")

        return final_sql


# Defining the plugin class
class AirflowRedshiftPlugin(AirflowPlugin):
    name = "postgres_to_redshift_operator"
    operators = [PostgresToRedshiftOperator]
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
