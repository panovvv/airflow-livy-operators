import json
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

ENDPOINT = "batches"
LOG_PAGE_LINES = 100


class LivyBatchSensor(BaseSensorOperator):
    def __init__(
        self,
        batch_id,
        poke_interval,
        timeout,
        task_id,
        http_conn_id="livy",
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
        self.batch_id = batch_id
        self.http_conn_id = http_conn_id

    def poke(self, context):
        logging.info(f"Getting batch {self.batch_id} status...")
        endpoint = f"{ENDPOINT}/{self.batch_id}"
        response = HttpHook(method="GET", http_conn_id=self.http_conn_id).run(endpoint)
        batch = json.loads(response.content)
        state = batch["state"]
        if (state == "starting") or (state == "running"):
            logging.info(
                f"Batch {self.batch_id} has not finished yet " f"(state is '{state}')"
            )
            return False
        elif state == "success":
            return True
        else:
            raise AirflowException(f"Batch {self.batch_id} failed with state '{state}'")


# TODO DATACLASS?
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
        http_conn_id="livy",
        spill_logs=True,
        *args,
        **kwargs,
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
        self.http_conn_id = http_conn_id
        self.spill_logs = spill_logs

    def execute(self, context):
        """
        1. Submit a batch to Livy
        2. Poll API until it's ready
        """
        batch_id = None
        try:
            batch_id = self.submit_batch()
            logging.info(f"Batch successfully submitted with id = {batch_id}.")
            LivyBatchSensor(
                batch_id,
                task_id=self.task_id,
                http_conn_id=self.http_conn_id,
                poke_interval=self.poll_period_sec,
                timeout=self.timeout_minutes * 60,
            ).execute(context)
        except AirflowException:
            if batch_id:
                self.spill_batch_logs(batch_id)
            raise
        finally:
            if batch_id:
                if self.spill_logs:
                    self.spill_batch_logs(batch_id)
                self.close_batch(batch_id)

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
        logging.info(f"Submitting a batch to Livy... "
                     f"Payload:\n{json.dumps(payload, indent=2)}")
        response = HttpHook(http_conn_id=self.http_conn_id).run(
            ENDPOINT, json.dumps(payload), headers
        )
        return json.loads(response.content)["id"]

    def spill_batch_logs(self, batch_id):
        dashes = 50
        logging.info(f"{'-'*dashes}Full log for batch {batch_id}{'-'*dashes}")
        endpoint = f"{ENDPOINT}/{batch_id}/log?from="
        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        line_from = 0
        line_to = LOG_PAGE_LINES
        while True:
            prepd_endpoint = endpoint + f"{line_from}&size={line_to}"
            response = json.loads(hook.run(prepd_endpoint).content)
            logs = response["log"]
            for log in logs:
                logging.info(log.replace("\\n", "\n"))
            actual_line_from = response["from"]
            total_lines = response["total"]
            actual_lines = len(logs)
            if actual_line_from + actual_lines >= total_lines:
                logging.info(
                    f"{'-' * dashes}End of full log for batch {batch_id}{'-' * dashes}"
                )
                break
            line_from = actual_line_from + actual_lines

    def close_batch(self, batch_id):
        logging.info(f"Closing batch with id = {batch_id}")
        batch_endpoint = f"{ENDPOINT}/{batch_id}"
        HttpHook(method="DELETE", http_conn_id=self.http_conn_id).run(batch_endpoint)
        logging.info(f"Batch {batch_id} has been closed")


class LivyBatchPlugin(AirflowPlugin):
    name = "Livy batch plugin"
    operators = [LivyBatchOperator]
    sensors = [LivyBatchSensor]
