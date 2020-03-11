from airflow.plugins_manager import AirflowPlugin

from .batch import LivyBatchOperator, LivyBatchSensor
from .session import (
    LivySessionCreationSensor,
    LivySessionOperator,
    LivyStatementSensor,
)


class LivyBatchPlugin(AirflowPlugin):
    name = "Livy batch plugin"
    sensors = [LivyBatchSensor]
    operators = [LivyBatchOperator]


class LivySessionPlugin(AirflowPlugin):
    name = "Livy session plugin"
    sensors = [LivySessionCreationSensor, LivyStatementSensor]
    operators = [LivySessionOperator]
