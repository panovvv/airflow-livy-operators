from airflow.plugins_manager import AirflowPlugin

try:
    # Runtime import: example DAGs and PyPi
    from airflow_livy.batch import LivyBatchOperator, LivyBatchSensor
    from airflow_livy.session import (
        LivySessionCreationSensor,
        LivySessionOperator,
        LivyStatementSensor,
    )
except ImportError:
    # Static import: so that IDE sees it, also for tests.
    from airflow_home.plugins.airflow_livy.batch import (
        LivyBatchOperator,
        LivyBatchSensor,
    )
    from airflow_home.plugins.airflow_livy.session import (
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
