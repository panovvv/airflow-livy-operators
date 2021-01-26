"""
Workflow:
1. Submit a batch to Livy.
2. Poll API until it's ready.
3. If an additional verification method is specified, retrieve the job status
there and disregard the batch status from Livy.

Supported verification methods are Spark/YARN REST API.
When the batch job is running in cluster mode on YARN cluster,
it sometimes shows as "succeeded" even when underlying job fails.

https://livy.incubator.apache.org/docs/latest/rest-api.html
https://spark.apache.org/docs/latest/monitoring.html#rest-api
https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
"""

import json
import logging
from json import JSONDecodeError
from numbers import Number

from airflow.exceptions import AirflowBadRequest, AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

LIVY_ENDPOINT = "batches"
SPARK_ENDPOINT = "api/v1/applications"
YARN_ENDPOINT = "ws/v1/cluster/apps"
VERIFICATION_METHODS = ["spark", "yarn"]
VALID_BATCH_STATES = [
    "not_started",
    "starting",
    "recovering",
    "idle",
    "running",
    "busy",
    "shutting_down",
]
LOG_PAGE_LINES = 100


def log_response_error(lookup_path, response, batch_id=None):
    msg = "Can not parse JSON response."
    if batch_id is not None:
        msg += " Batch id={batch_id}.".format(batch_id=batch_id)
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


class LivyBatchSensor(BaseSensorOperator):
    def __init__(
        self,
        batch_id,
        task_id,
        poke_interval=20,
        timeout=10 * 60,
        http_conn_id="livy",
        soft_fail=False,
        mode="poke",
    ):
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
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            soft_fail=soft_fail,
            mode=mode,
            task_id=task_id,
        )
        self.batch_id = batch_id
        self.http_conn_id = http_conn_id

    def poke(self, context):
        logging.info("Getting batch {batch_id} status...".format(batch_id=self.batch_id))
        endpoint = "{LIVY_ENDPOINT}/{batch_id}".format(LIVY_ENDPOINT=LIVY_ENDPOINT, batch_id=self.batch_id)
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        try:
            state = json.loads(response.content)["state"]
        except (JSONDecodeError, LookupError) as ex:
            log_response_error("$.state", response, self.batch_id)
            raise AirflowBadRequest(ex)
        if state in VALID_BATCH_STATES:
            logging.info(
                "Batch {batch_id} has not finished yet (state is '{state}')"
                    .format(batch_id=self.batch_id, state=state)
            )
            return False
        if state == "success":
            logging.info("Batch {batch_id} has finished successfully!".format(batch_id=self.batch_id))
            return True
        raise AirflowException("Batch {batch_id} failed with state '{state}'"
                               .format(batch_id=self.batch_id, state=state))


class LivyBatchOperator(BaseOperator):
    template_fields = ["name", "arguments"]

    @apply_defaults
    def __init__(
        self,
        file=None,
        proxy_user=None,
        class_name=None,
        arguments=None,
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
        timeout_minutes=10,
        poll_period_sec=20,
        verify_in=None,
        http_conn_id_livy="livy",
        http_conn_id_spark="spark",
        http_conn_id_yarn="yarn",
        spill_logs=True,
        *args,
        **kwargs
    ):
        super(LivyBatchOperator, self).__init__(*args, **kwargs)
        self.file = file
        self.proxy_user = proxy_user
        self.class_name = class_name
        self.arguments = arguments
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
        self.timeout_minutes = timeout_minutes
        self.poll_period_sec = poll_period_sec
        if verify_in in VERIFICATION_METHODS or verify_in is None:
            self.verify_in = verify_in
        else:
            raise AirflowException(
                "Can not create batch operator with verification method "
                "'{verify_in}'!\nAllowed methods: {VERIFICATION_METHODS}"
                    .format(verify_in=verify_in, VERIFICATION_METHODS=VERIFICATION_METHODS)
            )
        self.http_conn_id_livy = http_conn_id_livy
        self.http_conn_id_spark = http_conn_id_spark
        self.http_conn_id_yarn = http_conn_id_yarn
        self.spill_logs = spill_logs
        self.batch_id = None

    def execute(self, context):
        try:
            self.submit_batch()
            logging.info("Batch successfully submitted with id = {batch_id}.".format(batch_id=self.batch_id))
            LivyBatchSensor(
                self.batch_id,
                task_id=self.task_id,
                http_conn_id=self.http_conn_id_livy,
                poke_interval=self.poll_period_sec,
                timeout=self.timeout_minutes * 60,
            ).execute(context)
            if self.verify_in in VERIFICATION_METHODS:
                logging.info(
                    "Additionally verifying status for batch id {batch_id} "
                    "via {verify_in}..."
                        .format(batch_id=self.batch_id, verify_in=self.verify_in)
                )
                self.verify()
        except Exception:
            if self.batch_id is not None:
                self.spill_batch_logs()
                self.close_batch()
                self.batch_id = None
            raise
        finally:
            if self.batch_id is not None:
                if self.spill_logs:
                    self.spill_batch_logs()
                self.close_batch()

    def submit_batch(self):
        headers = {"X-Requested-By": "airflow", "Content-Type": "application/json"}
        unfiltered_payload = {
            "file": self.file,
            "proxyUser": self.proxy_user,
            "className": self.class_name,
            "args": self.arguments,
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
        }
        payload = {k: v for k, v in unfiltered_payload.items() if v}
        logging.info(
            "Submitting the batch to Livy... "
            "Payload:\n{payload}"
                .format(payload=json.dumps(payload, indent=2))
        )
        response = HttpHook(http_conn_id=self.http_conn_id_livy).run(
            LIVY_ENDPOINT, json.dumps(payload), headers
        )
        try:
            batch_id = json.loads(response.content)["id"]
        except (JSONDecodeError, LookupError) as ex:
            log_response_error("$.id", response)
            raise AirflowBadRequest(ex)
        if not isinstance(batch_id, Number):
            raise AirflowException(
                "ID of the created batch is not a number ({batch_id}). "
                "Are you sure we're calling Livy API?"
                    .format(batch_id=batch_id)
            )
        self.batch_id = batch_id

    def verify(self):
        app_id = self.get_spark_app_id(self.batch_id)
        if app_id is None:
            raise AirflowException("Spark appId was null for batch {batch_id}".format(batch_id=self.batch_id))
        logging.info("Found app id '{app_id}' for batch id {batch_id}.".format(app_id=app_id, batch_id=self.batch_id))
        if self.verify_in == "spark":
            self.check_spark_app_status(app_id)
        else:
            self.check_yarn_app_status(app_id)
        logging.info("App '{app_id}' associated with batch {self.batch_id} completed!"
                     .format(app_id=app_id, batch_id=self.batch_id))

    def get_spark_app_id(self, batch_id):
        logging.info("Getting Spark app id from Livy API for batch {batch_id}...".format(batch_id=batch_id))
        endpoint = "{LIVY_ENDPOINT}/{batch_id}".format(LIVY_ENDPOINT=LIVY_ENDPOINT, batch_id=batch_id)
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id_livy).run(
            endpoint
        )
        try:
            return json.loads(response.content)["appId"]
        except (JSONDecodeError, LookupError, AirflowException) as ex:
            log_response_error("$.appId", response, batch_id)
            raise AirflowBadRequest(ex)

    def check_spark_app_status(self, app_id):
        logging.info("Getting app status (id={app_id}) from Spark REST API...".format(app_id=app_id))
        endpoint = "{SPARK_ENDPOINT}/{app_id}/jobs".format(SPARK_ENDPOINT=SPARK_ENDPOINT, app_id=app_id)
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id_spark).run(
            endpoint
        )
        try:
            jobs = json.loads(response.content)
            expected_status = "SUCCEEDED"
            for job in jobs:
                job_id = job["jobId"]
                job_status = job["status"]
                logging.info(
                    "Job id {job_id} associated with application '{app_id}' "
                    "is '{job_status}'".format(app_id=app_id, job_status=job_status)
                )
                if job_status != expected_status:
                    raise AirflowException(
                        "Job id '{job_id}' associated with application '{app_id}' "
                        "is '{job_status}', expected status is '{expected_status}'"
                            .format(job_id=job_id, app_id=app_id, job_status=job_status, expected_status=expected_status)
                    )
        except (JSONDecodeError, LookupError, TypeError) as ex:
            log_response_error("$.jobId, $.status", response)
            raise AirflowBadRequest(ex)

    def check_yarn_app_status(self, app_id):
        logging.info("Getting app status (id={app_id}) from YARN RM REST API...".format(app_id=app_id))
        endpoint = "{YARN_ENDPOINT}/{app_id}".format(YARN_ENDPOINT=YARN_ENDPOINT, app_id=app_id)
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id_yarn).run(
            endpoint
        )
        try:
            status = json.loads(response.content)["app"]["finalStatus"]
        except (JSONDecodeError, LookupError, TypeError) as ex:
            log_response_error("$.app.finalStatus", response)
            raise AirflowBadRequest(ex)
        expected_status = "SUCCEEDED"
        if status != expected_status:
            raise AirflowException(
                "YARN app {app_id} is '{status}', expected status: '{expected_status}'"
                    .format(app_id=app_id, status=status, expected_status=expected_status)
            )

    def spill_batch_logs(self):
        dashes = '-'*50
        logging.info("{dashes}Full log for batch {batch_id}{dashes}".format(dashes=dashes, batch_id=self.batch_id))
        endpoint = "{LIVY_ENDPOINT}/{batch_id}/log".format(LIVY_ENDPOINT=LIVY_ENDPOINT, batch_id=self.batch_id)
        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id_livy)
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
                log_response_error("$.log, $.from, $.total", log_page)
                raise AirflowBadRequest(ex)
            actual_lines = len(logs)
            if actual_line_from + actual_lines >= total_lines:
                logging.info(
                    "{dashes}End of full log for batch {batch_id}"
                    "{dashes}".format(dashes=dashes, batch_id=self.batch_id)
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

    def close_batch(self):
        logging.info("Closing batch with id = {batch_id}".format(batch_id=self.batch_id))
        batch_endpoint = "{LIVY_ENDPOINT}/{batch_id}".format(LIVY_ENDPOINT=LIVY_ENDPOINT, batch_id=self.batch_id)
        HttpHook(method="DELETE", http_conn_id=self.http_conn_id_livy).run(
            batch_endpoint
        )
        logging.info("Batch {batch_id} has been closed".format(batch_id=self.batch_id))
