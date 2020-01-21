import json
import os
from typing import Tuple, Dict, List

from jinja2 import Template
from slackclient import SlackClient

from airflow.plugins_manager import AirflowPlugin
from airflow.operators import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException
from airflow.models import Variable


# Plugin configs

# path to your Healthcheck folder
HEALTHCHECKS_PATH = os.path.join(
    os.path.abspath(__file__), "..", "..", "src", "healthcheck"
)

# database hook used for Healthcheck, data warehouse specific
DW_HOOK = PostgresHook
PRIMARY_SLACK_NOTIFIER = ["user"]

# Healthcheck reporting slack channel
slack_token = Variable.get("slack_token")
slack_channel = Variable.get("slack_data_healthcheck")


class BaseHealthCheckOperator(BaseOperator):

    """
    Base class for Data Healthcheck Operator.

    It wraps around any types of transformation operators (e.g. BashOperator, PostgresOperator)
    and perform raw SQL healthcheck after the transformation.

    The SQLs (test cases) are grouped based on data warehouse structure.

    Recommended structure:

    ```
    - HEALTHCHECKS_PATH (declared above)
        - schema1_name
            - table1_name.sql
            - table2_name.sql
            - table3_name.sql
            - table4_name.sql
        - schema2_name
            ...
    ```

    Each SQL file would contain multiple SQL queries, separated by `-- name:` tags.

    Types of checks implemented so far:

    1. checking existence
        - the result of test sql should be empty
        - useful for comparing snapshots

    2. checking value
        - the result of test sql should be a single number
        - useful for comparing ranges, exact values, duplicates

    Arguments:
        - dw_conn_id: str, Airflow connection id for Data Warehouse
        - test_sql: str, relative path to the test SQL file, relative to the
            HEALTHCHECKS_PATH config declared above
        - test_reference: dict, additional arguments for the tests
        - block_on_failure: bool, failed the transformation task and blocks the
            subsequent loading task if tests failed
        - verbose: bool, sent more verbose message

    Example usage:

    task = HealthCheckBashOperator(
        task_id='foo',
        dw_conn_id='test-conn',
        bash_command="echo 'hello'",
        test_sql='schema_name/table_name.sql',
        test_reference={
            'hello': {
                'type': 'existence'
            },
            'hello2': {
                'type': 'comparison',
                'operator': 'eq',
                'value': 43
            }
        },
        verbose=True,
        block_on_failure=True,
        dag=dag
    )
    """

    ui_color = "#FFFFFF"  # white

    @apply_defaults
    def __init__(
        self,
        dw_conn_id: str,
        test_sql: str,
        test_reference: dict = None,
        block_on_failure: bool = False,
        verbose: bool = False,
        users_to_alert: Tuple = (),
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.dw_hook = DW_HOOK(dw_conn_id)
        self.test_sql = os.path.abspath(os.path.join(HEALTHCHECKS_PATH, test_sql))
        self.test_reference = test_reference
        self.block_on_failure = block_on_failure
        self.verbose = verbose
        self.users_to_alert = list(users_to_alert)

        self.users_to_alert.extend(PRIMARY_SLACK_NOTIFIER)

        # initialise
        self._test_queries = {}
        self._assertions = []

    def _load_test_queries(self, context):
        """
        load and render the test queries from path declared in the task
        definition
        """
        self.log.warning("Preparing test queries ...")

        result = {}
        with open(self.test_sql, "r") as test_file:
            test_string = test_file.read()
            rows = [r for r in test_string.split("\n")]  # split by lines
            current_key = ""
            for row in rows:
                if ("commit" or "begin") in row.lower():
                    continue
                elif "-- name:" in row:
                    current_key = row.split("-- name:")[1].strip().lower()

                    if current_key in result.keys():
                        raise AirflowException(
                            "Duplicated query key detected in one SQL file. "
                        )

                    result[current_key] = ""
                elif len(row) > 0 and current_key in result.keys():
                    result[current_key] += row + "\n"

        for query_name, query in result.items():
            result[query_name] = Template(result[query_name]).render(**context)

        self.log.warning(f"{len(result)} queries loaded and rendered. ")
        return result

    def _assert_resp(self, test_name: str, resp: Tuple[dict]) -> Tuple[bool, dict]:
        """ core assertion logic, assert the response from test sql """
        try:
            reference = self.test_reference[test_name]
        except KeyError:
            raise AirflowException("Test reference and test queries do not match.")

        assertion = None
        if reference["type"] == "existence":
            if resp:
                assertion = True
        elif reference["type"] == "comparison":
            value = reference["value"]
            operator = reference["operator"]

            if resp:
                resp = resp[0]  # unpack from tuple

            if operator == "eq":
                assertion = resp == value
            elif operator == "gte":
                assertion = resp >= value
            elif operator == "lte":
                assertion = resp <= value
            elif operator == "gt":
                assertion = resp > value
            elif operator == "lt":
                assertion = resp < value
            else:
                raise AirflowException("Unknown comparison sign. ")
        else:
            raise AirflowException("Unknown Healthcheck type. ")

        return assertion, {"reference": reference, "actual": resp}

    def _report_assertions(
        self,
        attachments: List[Dict[str, str]],
        successful_test_count: int,
        total_tests_count: int,
        context: dict,
    ):
        """
        construct post message and report to slack
        :param attachments:
        :param successful_test_count:
        :param total_tests_count:
        :param context:
        :return:
        """
        if successful_test_count != total_tests_count:
            status_color = ":x:"
            notification_list = ", ".join(
                [f"<@{user}>" for user in self.users_to_alert]
            )
            alerting_text = f"Alerting: {notification_list}"
        else:
            status_color = ":white_check_mark:"
            alerting_text = ""

        main_text = (
            f'Task health: {status_color}  - {context.get("task_instance").task_id}:'
            f" {successful_test_count} / {total_tests_count} \n {alerting_text}"
        )

        api_params = {
            "channel": slack_channel,
            "username": "DW Healthchecker",
            "text": main_text,
            "icon_url": "https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png",
            "attachments": json.dumps(attachments),
        }

        sc = SlackClient(slack_token)
        rc = sc.api_call(method="chat.postMessage", **api_params)

        self.log.warning(rc)

    def _aggregate_test_results(self):
        """
        aggregate test cases result and construct
        messages
        :return:
        """
        status_color = {True: "#2eb886", False: "#d61922"}
        attachments = []
        total_tests = len(self._assertions)
        successful_tests = 0
        for assertion in self._assertions:
            test_status = assertion["test_status"]
            test_name = assertion["test_name"]
            test_info = assertion["test_info"]

            displayed_message = f"Test case: {test_name}"
            displayed_color = status_color[test_status]

            current_attachment = {"color": displayed_color, "title": displayed_message}

            if self.verbose:
                if not test_status:
                    current_attachment["text"] = str(test_info)
                attachments.append(current_attachment)  # only print all in verbose mode
            else:
                if not test_status:
                    current_attachment["text"] = str(test_info)
                    attachments.append(current_attachment)

            if test_status:
                successful_tests += 1

        return attachments, successful_tests, total_tests

    def execute(self, context):
        self._test_queries = self._load_test_queries(context)

        # actual execution
        self._actual_execution(context)

        # health checking the results
        for test_name, test_query in self._test_queries.items():
            resp = self.dw_hook.get_first(test_query)
            assertion = self._assert_resp(test_name, resp)
            self._format_assertion(assertion, test_name)

        slack_result_attachments, successful_tests_count, total_tests_count = (
            self._aggregate_test_results()
        )
        suite_status = successful_tests_count == total_tests_count

        if not context["test_mode"]:
            # not in test mode, report
            self._report_assertions(
                attachments=slack_result_attachments,
                successful_test_count=successful_tests_count,
                total_tests_count=total_tests_count,
                context=context,
            )
        else:
            self.log.warning(
                f"Reporting to Slack is disabled during test / backfill mode. "
                f"Showing failure result attachments below: "
            )

            self.log.warning(slack_result_attachments)

        if self.block_on_failure and not suite_status:
            raise AirflowException("Blocking this task due to healthcheck failure. ")

        self.log.warning("Task completed.")

    def _format_assertion(self, assertion, test_name):
        self._assertions.append(
            {
                "test_name": test_name,
                "test_status": assertion[0],
                "test_info": assertion[1],
            }
        )

    def _actual_execution(self, context):
        raise NotImplementedError


# register your operators below
# Some commonly used Operators are pre-registered
class HealthCheckBashOperator(BaseHealthCheckOperator, BashOperator):

    template_fields = BashOperator.template_fields
    template_ext = BashOperator.template_ext
    ui_color = "#ffeead"

    def _actual_execution(self, context):
        BashOperator.execute(self, context)


class HealthCheckSSHOperator(BaseHealthCheckOperator, SSHOperator):

    template_fields = SSHOperator.template_fields
    template_ext = SSHOperator.template_ext
    ui_color = "#ffeead"

    def _actual_execution(self, context):
        SSHOperator.execute(self, context)


class HealthCheckPostgresOperator(BaseHealthCheckOperator, PostgresOperator):

    template_fields = PostgresOperator.template_fields
    template_ext = PostgresOperator.template_ext
    ui_color = "#ffeead"

    def _actual_execution(self, context):
        PostgresOperator.execute(self, context)


# Defining the plugin class
class AirflowHealthCheckPlugin(AirflowPlugin):
    name = "healthcheck_operator"
    operators = [
        HealthCheckBashOperator,
        HealthCheckSSHOperator,
        HealthCheckPostgresOperator,
    ]
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
