"""
Workflow:
1. Create a session in Livy
2. Poll API until it's ready to execute statements
3. Submit provided statements to the session, one by one
4. For each submitted statement, poll API until it's completed or failed.
   If one of the statements fail, do not proceed with the remaining ones.
5. Close the session.

https://livy.incubator.apache.org/docs/latest/rest-api.html
"""

import json
import logging
from json import JSONDecodeError
from numbers import Number
from typing import List

from airflow.exceptions import AirflowBadRequest, AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

ENDPOINT = "sessions"
ALLOWED_LANGUAGES = ["spark", "pyspark", "sparkr", "sql"]
LOG_PAGE_LINES = 100


def log_response_error(lookup_path, response, session_id=None, statement_id=None):
    msg = "Can not parse JSON response."
    if session_id is not None:
        msg += " Session id={session_id}.".format(session_id=session_id)
    if statement_id is not None:
        msg += " Statement id={statement_id}.".format(statement_id=statement_id)
    try:
        pp_response = (
            json.dumps(json.loads(response.content), indent=2)
            if "application/json" in response.headers.get("Content-Type", "")
            else response.content
        )
    except AttributeError:
        pp_response = json.dumps(response, indent=2)
    msg += "\nTried to find JSON path: {lookup_path}, but response was:\n{pp_response}"\
        .format(lookup_path=lookup_path, pp_response=pp_response)
    logging.error(msg)


def validate_timings(poke_interval, timeout):
    if poke_interval < 1:
        raise AirflowException(
            "Poke interval {poke_interval} sec. is too small, "
            "this will result in too frequent API calls"
                .format(poke_interval=poke_interval)
        )
    if poke_interval > timeout:
        raise AirflowException(
            "Poke interval {poke_interval} sec. is greater "
            "than the timeout value {timeout} sec. Timeout won't work."
                .format(poke_interval=poke_interval, timeout=timeout)
        )


class LivySessionCreationSensor(BaseSensorOperator):
    def __init__(
        self,
        session_id,
        poke_interval,
        timeout,
        task_id,
        http_conn_id="livy",
        soft_fail=False,
        mode="poke",
    ):
        validate_timings(poke_interval, timeout)
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
        logging.info("Getting session {session_id} status...".format(session_id=self.session_id))
        endpoint = "{ENDPOINT}/{session_id}/state".format(ENDPOINT=ENDPOINT, session_id=self.session_id)
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        try:
            state = json.loads(response.content)["state"]
        except (JSONDecodeError, LookupError) as ex:
            log_response_error("$.state", response, self.session_id)
            raise AirflowBadRequest(ex)
        if state == "starting":
            logging.info("Session {session_id} is starting...".format(session_id=self.session_id))
            return False
        if state == "idle":
            logging.info("Session {session_id} is ready to receive statements.".format(session_id=self.session_id))
            return True
        raise AirflowException(
            "Session {session_id} failed to start. "
            "State='{state}'. Expected states: 'starting' or 'idle' (ready).".format(session_id=self.session_id, state=state)
        )


class LivyStatementSensor(BaseSensorOperator):
    def __init__(
        self,
        session_id,
        statement_id,
        poke_interval,
        timeout,
        task_id,
        http_conn_id="livy",
        soft_fail=False,
        mode="poke",
    ):
        validate_timings(poke_interval, timeout)
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
            "Getting status for statement {statement_id} "
            "in session {session_id}".format(statement_id=self.statement_id, session_id=self.session_id)
        )
        endpoint = "{ENDPOINT}/{session_id}/statements/{statement_id}"\
            .format(ENDPOINT=ENDPOINT, session_id=self.session_id, statement_id=self.statement_id)
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        try:
            statement = json.loads(response.content)
            state = statement["state"]
        except (JSONDecodeError, LookupError) as ex:
            log_response_error("$.state", response, self.session_id, self.statement_id)
            raise AirflowBadRequest(ex)
        if state in ["waiting", "running"]:
            logging.info(
                "Statement {statement_id} in session {session_id} "
                "has not finished yet (state is '{state}')"
                    .format(statement_id=self.statement_id, session_id=self.session_id, state=state)
            )
            return False
        if state == "available":
            self.__check_status(statement, response)
            return True
        raise AirflowBadRequest(
            "Statement {statement_id} in session {session_id} failed due to "
            "an unknown state: '{state}'.\nKnown states: 'waiting', 'running', "
            "'available'"
                .format(statement_id=self.statement_id, session_id=self.session_id, state=state)
        )

    def __check_status(self, statement, response):
        try:
            output = statement["output"]
            status = output["status"]
        except LookupError as ex:
            log_response_error(
                "$.output.status", response, self.session_id, self.statement_id
            )
            raise AirflowBadRequest(ex)
        pp_output = "\n".join(json.dumps(output, indent=2).split("\\n"))
        logging.info(
            "Statement {statement_id} in session {session_id} "
            "finished. Output:\n{pp_output}"
                .format(statement_id=self.statement_id, session_id=self.session_id, pp_output=pp_output)
        )
        if status != "ok":
            raise AirflowBadRequest(
                "Statement {statement_id} in session {session_id} "
                "failed with status '{status}'. Expected status is 'ok'"
                    .format(statement_id=self.statement_id, session_id=self.session_id, status=status)
            )


class LivySessionOperator(BaseOperator):
    class Statement:
        template_fields = ["code"]
        code = ""
        kind = ""

        def __init__(self, code, kind=None):
            if kind in ALLOWED_LANGUAGES or kind is None:
                self.kind = kind
            else:
                raise AirflowException(
                    "Can not create statement with kind '{kind}'!\n"
                    "Allowed session kinds: {ALLOWED_LANGUAGES}"
                        .format(kind=kind, ALLOWED_LANGUAGES=ALLOWED_LANGUAGES)
                )
            self.code = code

        def __str__(self) -> str:
            dashes = '-'*80
            return (
                "\n{{\n  Statement, kind: {kind}"
                "\n  code:\n{dashes}\n{code}\n{dashes}\n}}"
                    .format(kind=self.kind, code=self.code, dashes=dashes)
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
        session_start_timeout_sec=120,
        session_start_poll_period_sec=10,
        statemt_timeout_minutes=10,
        statemt_poll_period_sec=20,
        http_conn_id="livy",
        spill_logs=False,
        *args,
        **kwargs
    ):
        super(LivySessionOperator, self).__init__(*args, **kwargs)
        if kind in ALLOWED_LANGUAGES or kind is None:
            self.kind = kind
        else:
            raise AirflowException(
                "Can not create session with kind '{kind}'!\n"
                "Allowed session kinds: {ALLOWED_LANGUAGES}"
                    .format(kind=kind, ALLOWED_LANGUAGES=ALLOWED_LANGUAGES)
            )
        self.statements = statements
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
        self.spill_logs = spill_logs
        self.session_id = None

    def execute(self, context):
        try:
            self.create_session()
            logging.info("Session has been created with id = {session_id}.".format(session_id=self.session_id))
            LivySessionCreationSensor(
                self.session_id,
                task_id=self.task_id,
                http_conn_id=self.http_conn_id,
                poke_interval=self.session_start_poll_period_sec,
                timeout=self.session_start_timeout_sec,
            ).execute(context)
            logging.info("Session {session_id} is ready to accept statements.".format(session_id=self.session_id))
            for i, statement in enumerate(self.statements):
                logging.info(
                    "Submitting statement {i+1}/{len(statements)} "
                    "in session {session_id}..."
                        .format(statements=self.statements, session_id=self.session_id)
                )
                statement_id = self.submit_statement(statement)
                logging.info(
                    "Statement {i+1}/{len(statements)} "
                    "(session {session_id}) "
                    "has been submitted with id {statement_id}"
                        .format(statements=self.statements, session_id=self.session_id, statement_id=statement_id)
                )
                LivyStatementSensor(
                    self.session_id,
                    statement_id,
                    task_id=self.task_id,
                    http_conn_id=self.http_conn_id,
                    poke_interval=self.statemt_poll_period_sec,
                    timeout=self.statemt_timeout_minutes * 60,
                ).execute(context)
            logging.info(
                "All {len(statements)} statements in session {session_id} "
                "completed successfully!"
                    .format(statements=self.statements, session_id=self.session_id)
            )
        except Exception:
            if self.session_id is not None:
                self.spill_session_logs()
                self.spill_logs = False
            raise
        finally:
            if self.session_id is not None:
                if self.spill_logs:
                    self.spill_session_logs()
                self.close_session()

    def create_session(self):
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
        logging.info(
            "Creating a session in Livy... "
            "Payload:\n{payload}".format(payload=json.dumps(payload, indent=2))
        )
        response = HttpHook(http_conn_id=self.http_conn_id).run(
            ENDPOINT, json.dumps(payload), headers,
        )
        try:
            session_id = json.loads(response.content)["id"]
        except (JSONDecodeError, LookupError) as ex:
            log_response_error("$.id", response)
            raise AirflowBadRequest(ex)

        if not isinstance(session_id, Number):
            raise AirflowException(
                "ID of the created session is not a number ({session_id}). "
                "Are you sure we're calling Livy API?"
                    .format(session_id=session_id)
            )
        self.session_id = session_id

    def submit_statement(self, statement: Statement):
        headers = {"X-Requested-By": "airflow", "Content-Type": "application/json"}
        payload = {"code": statement.code}
        if statement.kind:
            payload["kind"] = statement.kind
        endpoint = "{ENDPOINT}/{session_id}/statements".format(ENDPOINT=ENDPOINT, session_id=self.session_id)
        response = HttpHook(http_conn_id=self.http_conn_id).run(
            endpoint, json.dumps(payload), headers
        )
        try:
            return json.loads(response.content)["id"]
        except (JSONDecodeError, LookupError) as ex:
            log_response_error("$.id", response, self.session_id)
            raise AirflowBadRequest(ex)

    def spill_session_logs(self):
        dashes = '-'*50
        logging.info("{dashes}Full log for session {session_id}{dashes}".format(dashes=dashes, session_id=self.session_id))
        endpoint = "{ENDPOINT}/{session_id}/log".format(ENDPOINT=ENDPOINT, session_id=self.session_id)
        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        line_from = 0
        line_to = LOG_PAGE_LINES
        while True:
            log_page = self.fetch_log_page(hook, endpoint, line_from, line_to)
            try:
                logs = log_page["log"]
                for log in logs:
                    logging.info(log.replace("\\n", "\n"))
                actual_line_from = log_page["from"]
                total_lines = log_page["total"]
            except LookupError as ex:
                log_response_error("$.log, $.from, $.total", log_page, self.session_id)
                raise AirflowBadRequest(ex)
            actual_lines = len(logs)
            if actual_line_from + actual_lines >= total_lines:
                logging.info(
                    "{dashes}End of full log for session {session_id}"
                    "{dashes}".format(dashes=dashes, session_id=self.session_id)
                )
                break
            line_from = actual_line_from + actual_lines

    @staticmethod
    def fetch_log_page(hook: HttpHook, endpoint, line_from, line_to):
        prepd_endpoint = endpoint + "?from={line_from}&size={line_to}".format(line_from=line_from, line_to=line_to)
        response = hook.run(prepd_endpoint)
        try:
            return json.loads(response.content)
        except JSONDecodeError as ex:
            log_response_error("$", response)
            raise AirflowBadRequest(ex)

    def close_session(self):
        logging.info("Closing session with id = {session_id}".format(session_id=self.session_id))
        session_endpoint = "{ENDPOINT}/{session_id}".format(ENDPOINT=ENDPOINT, session_id=self.session_id)
        HttpHook(method="DELETE", http_conn_id=self.http_conn_id).run(session_endpoint)
        logging.info("Session {session_id} has been closed".format(session_id=self.session_id))
