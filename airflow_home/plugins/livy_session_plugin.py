import json
import logging
from typing import List

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

ENDPOINT = "sessions"

ALLOWED_LANGUAGES = ["spark", "pyspark", "sparkr", "sql"]

LOG_PAGE_SIZE = 100


class LivySessionCreationSensor(BaseSensorOperator):
    def __init__(
        self,
        session_id,
        task_id,
        http_conn_id="livy",
        poke_interval=60,
        timeout=60 * 60 * 24 * 7,
        soft_fail=False,
        mode="poke",
    ):
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            soft_fail=soft_fail,
            mode=mode,
            task_id=task_id,
        )
        self.session_id = session_id
        self.http_conn_id = http_conn_id

    def poke(self, context):
        logging.info(f"Getting session {self.session_id} status...")
        endpoint = f"{ENDPOINT}/{self.session_id}/state"
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        session = json.loads(response.content)
        state = session["state"]

        if state == "starting":
            logging.info(f"Session {self.session_id} is starting...")
            return False
        if state == "idle":
            logging.info(f"Session {self.session_id} is ready to receive statements.")
            return True
        raise AirflowException(f"Session {self.session_id} failed to start: '{state}'.")


class LivyStatementSensor(BaseSensorOperator):
    def __init__(
        self,
        session_id,
        statement_id,
        task_id,
        http_conn_id="livy",
        poke_interval=60,
        timeout=60 * 60 * 24 * 7,
        soft_fail=False,
        mode="poke",
    ):
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            soft_fail=soft_fail,
            mode=mode,
            task_id=task_id,
        )
        self.session_id = session_id
        self.statement_id = statement_id
        self.http_conn_id = http_conn_id

    def poke(self, context):
        logging.info(
            f"Getting status for statement {self.statement_id} "
            f"in session {self.session_id}"
        )
        endpoint = f"{ENDPOINT}/{self.session_id}/statements/{self.statement_id}"
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        statement = json.loads(response.content)
        state = statement["state"]

        if state in ["waiting", "running"]:
            logging.info(
                f"Statement {self.statement_id} in session {self.session_id} "
                f"has not finished yet (state is '{state}')"
            )
            return False
        if state == "available":
            output = statement["output"]
            status = output["status"]
            pp_output = "\n".join(json.dumps(output, indent=2).split("\\n"))
            logging.info(
                f"Statement {self.statement_id} in session {self.session_id} "
                f"finished:\n{pp_output}"
            )
            if status != "ok":
                raise AirflowException(
                    f"Statement {self.statement_id} in session {self.session_id} "
                    f"failed with status '{status}'"
                )
            return True
        raise AirflowException(
            f"Statement {self.statement_id} in session {self.session_id} failed due to "
            f"unknown state: '{state}'. Response was:\n{json.dumps(statement, indent=2)}"
        )


class LivySessionOperator(BaseOperator):
    class Statement:
        template_fields = ["code"]
        code: str
        kind: str

        def __init__(self, code, kind=None):
            if kind in ALLOWED_LANGUAGES or kind is None:
                self.kind = kind
            else:
                raise AirflowException(
                    f"Can not create statement with kind '{kind}'!\n"
                    f"Allowed session kinds: {ALLOWED_LANGUAGES}"
                )
            self.code = code

        def __str__(self) -> str:
            return (
                f"\n{{\n  Statement, kind: {self.kind}\n  code:\n{self.code}\n}}"
            )

        __repr__ = __str__

    template_fields = [
        "name",
        "statements",
    ]

    @apply_defaults
    def __init__(
        self,
        statements: List[Statement],
        kind: str = None,
        proxy_user=None,
        jars=None,
        py_files=None,
        files=None,
        driver_memory=None,
        driver_cores=None,
        executor_memory=None,
        executor_cores=None,
        num_executors=None,
        archives=None,
        queue=None,
        name=None,
        conf=None,
        heartbeat_timeout=None,
        session_start_timeout_sec=46,
        session_start_poll_period_sec=5,
        statemt_timeout_minutes=10,
        statemt_poll_period_sec=15,
        http_conn_id="livy",
        *args,
        **kwargs,
    ):
        super(LivySessionOperator, self).__init__(*args, **kwargs)
        self.statements = statements
        if kind in ALLOWED_LANGUAGES or kind is None:
            self.kind = kind
        else:
            raise AirflowException(
                f"Can not create session with kind '{kind}'!\n"
                f"Allowed session kinds: {ALLOWED_LANGUAGES}"
            )
        self.proxy_user = proxy_user
        self.jars = jars
        self.py_files = py_files
        self.files = files
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.archives = archives
        self.queue = queue
        self.name = name
        self.conf = conf
        self.heartbeat_timeout = heartbeat_timeout
        self.session_start_timeout_sec = session_start_timeout_sec
        self.session_start_poll_period_sec = session_start_poll_period_sec
        self.statemt_timeout_minutes = statemt_timeout_minutes
        self.statemt_poll_period_sec = statemt_poll_period_sec
        self.http_conn_id = http_conn_id

    def execute(self, context):
        """ TODO
        1. Creates a session in Livy
        2. Polls API until it's ready to execute statements
        3. Submits provided statement to the session in sequence
        4. Polls API until each to be completed or failed.
        5. Prints session log into airflow_code task log.
        """
        session_id = None
        try:
            session_id = self.create_session(context)
            logging.info(f"Session has been created with id = {session_id}.")
            LivySessionCreationSensor(
                session_id,
                task_id=self.task_id,
                http_conn_id=self.http_conn_id,
                poke_interval=self.session_start_poll_period_sec,
                timeout=self.session_start_timeout_sec,
            ).execute(context)
            logging.info(f"Session {session_id} is ready to accept statements.")
            for i, statement in enumerate(self.statements):
                logging.info(
                    f"Submitting statement {i+1}/{len(self.statements)} "
                    f"in session {session_id}..."
                )
                statement_id = self.submit_statement(session_id, statement)
                logging.info(
                    f"Statement {i+1}/{len(self.statements)} (session {session_id}) "
                    f"has been submitted with id {statement_id}"
                )
                LivyStatementSensor(
                    session_id,
                    statement_id,
                    task_id=self.task_id,
                    http_conn_id=self.http_conn_id,
                    poke_interval=self.statemt_poll_period_sec,
                    timeout=self.statemt_timeout_minutes,
                ).execute(context)
            logging.info(
                f"All {len(self.statements)} statements in session {session_id} "
                f"completed successfully!"
            )
        except AirflowException:
            if session_id:
                self.spill_session_logs(session_id)
            raise
        finally:
            if session_id:
                self.close_session(session_id)

    def create_session(self, context):
        headers = {"X-Requested-By": "airflow", "Content-Type": "application/json"}
        unfiltered_payload = {
            "kind": self.kind,
            "proxyUser": self.proxy_user,
            "jars": self.jars,
            "pyFiles": self.py_files,
            "files": self.files,
            "driverMemory": self.driver_memory,
            "driverCores": self.driver_cores,
            "executorMemory": self.executor_memory,
            "executorCores": self.executor_cores,
            "numExecutors": self.num_executors,
            "archives": self.archives,
            "queue": self.queue,
            "name": self.name,
            "conf": self.conf,
            "heartbeatTimeoutInSecond": self.heartbeat_timeout,
        }
        payload = {k: v for k, v in unfiltered_payload.items() if v}

        logging.info("Creating a session in Livy... Payload:")
        logging.info(json.dumps(payload, indent=2))
        response = HttpHook(http_conn_id=self.http_conn_id).run(
            ENDPOINT, json.dumps(payload), headers,
        )
        session = json.loads(response.content)
        return session["id"]

    def submit_statement(self, session_id, statement: Statement):
        headers = {"X-Requested-By": "airflow", "Content-Type": "application/json"}
        payload = {"kind": statement.kind, "code": statement.code}
        endpoint = f"{ENDPOINT}/{session_id}/statements"
        response = HttpHook(http_conn_id=self.http_conn_id).run(
            endpoint, json.dumps(payload), headers
        )
        statement = json.loads(response.content)
        statement_id = statement["id"]
        return statement_id

    def spill_session_logs(self, session_id):

        # TODO flip through pages
        logging.info("Full session logs:")
        endpoint = f"{ENDPOINT}/{session_id}/log?from=0"
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        logs = json.loads(response.content)["log"]
        for log in logs:
            logging.info(log)

    def close_session(self, session_id):
        logging.info(f"Closing session with id = {session_id}")
        session_endpoint = f"{ENDPOINT}/{session_id}"
        HttpHook(method="DELETE", http_conn_id=self.http_conn_id).run(session_endpoint)
        logging.info(f"Session {session_id} has been closed")


class LivySessionPlugin(AirflowPlugin):
    name = "Livy session plugin"
    operators = [LivySessionOperator]
    sensors = [LivySessionCreationSensor, LivyStatementSensor]
