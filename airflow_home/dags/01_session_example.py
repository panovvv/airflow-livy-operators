from datetime import datetime

from airflow import DAG

try:
    # Import statement for Airflow when it loads new operators into airflow.operators
    from airflow.operators import LivySessionOperator
except ImportError:
    # Import statement for IDE with the local folder structur
    from airflow_home.plugins.livy_session_plugin import LivySessionOperator

dag = DAG(
    "01_session_example",
    description="Running Spark jobs via Livy Sessions",
    schedule_interval=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
)


# See ready statements with parameter values substituted
# in the "Rendered template" tab of a running task.
scala_code = """
spark.range(1000 * 1000 * {{ params.your_number }}).count()
val df = Seq(
  ("One", 1),
  ("Two", 2),
  ("Three", 3),
  ("Four", 4)
).toDF("{{ params.your_string }}", "{{ run_id }}")
df.show()
"""

pyspark_code = """
import sys

print(sys.version)
spark.range(1000 * 1000 * {{params.your_number}}).count()
df = sqlContext.createDataFrame(
    [("One", 1), ("Two", 2), ("Three", 3), ("Four", 4)],
    ("{{ params.your_string }}", "{{ run_id }}"),
)
df.show()
"""

sparkr_code = """
df <- as.DataFrame(
    list("{{ params.your_number }}", "{{ run_id }}", "Three", "Four"),
    "{{ params.your_string }}")
head(df)
"""

sql_code = """
SELECT CONCAT('{{ params.your_string }}', ' in task instance ', '{{ run_id }}')
"""

t1 = LivySessionOperator(
    name="livy_session_example_{{ run_id }}",
    statements=[
        LivySessionOperator.Statement(code=scala_code, kind="spark"),
        LivySessionOperator.Statement(code=pyspark_code, kind="pyspark"),
        LivySessionOperator.Statement(code=sparkr_code, kind="sparkr"),
        LivySessionOperator.Statement(code=sql_code, kind="sql"),
    ],
    params={"your_number": 5, "your_string": "Hello world"},
    task_id="livy_session_example",
    dag=dag,
)
