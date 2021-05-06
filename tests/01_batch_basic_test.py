import json

from airflow import AirflowException
from airflow.exceptions import AirflowBadRequest
from airflow.providers.http.hooks.http import HttpHook
from deepdiff import DeepDiff
from pytest import mark, raises
from requests import Response

from airflow_home.plugins.airflow_livy.batch import LivyBatchOperator
from tests.helpers import find_json_in_args, mock_http_calls


def test_jinja(dag):
    op = LivyBatchOperator(
        name="test_jinja_{{ run_id }}",
        arguments=[
            "{{ run_id|replace(':', '-') }}",
            "prefix {{ custom_param }} postfix",
        ],
        task_id="test_jinja_batch",
        dag=dag,
    )
    op.render_template_fields({"run_id": "hello:world", "custom_param": "custom value"})
    assert op.name == "test_jinja_hello:world"
    assert op.arguments[0] == "hello-world"
    assert op.arguments[1] == "prefix custom value postfix"


def test_invalid_verification(dag):
    with raises(AirflowException) as ae:
        LivyBatchOperator(
            task_id="test_invalid_verification",
            verify_in="invalid",
            dag=dag,
        )
    print(
        f"\n\nCreated a batch operator with invalid veification method, "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_submit_batch_get_id(dag, mocker):
    op = LivyBatchOperator(task_id="test_submit_batch_get_id", dag=dag)
    http_response = mock_http_calls(201, content=b'{"id": 123}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    op.submit_batch()
    assert op.batch_id == 123


def test_submit_batch_params(dag, mocker):
    http_conn_id_yarn = "http_conn_id_yarn"
    http_conn_id_spark = "http_conn_id_spark"
    http_conn_id_livy = "http_conn_id_livy"
    timeout_minutes = 4
    poll_period_sec = 5
    verify_in = "yarn"
    op = LivyBatchOperator(
        file="file",
        proxy_user="proxy_user",
        class_name="class_name",
        arguments=["arg1", "arg2"],
        jars=["jar1", "jar2"],
        py_files=["py_file1", "py_file2"],
        files=["file1", "file2"],
        driver_memory="driver_memory",
        driver_cores=1,
        executor_memory="executor_memory",
        executor_cores=2,
        num_executors=3,
        archives=["archive1", "archive2"],
        queue="queue",
        name="name",
        conf={"key1": "val1", "key2": 2},
        timeout_minutes=timeout_minutes,
        poll_period_sec=poll_period_sec,
        verify_in=verify_in,
        http_conn_id_livy=http_conn_id_livy,
        http_conn_id_spark=http_conn_id_spark,
        http_conn_id_yarn=http_conn_id_yarn,
        task_id="test_submit_batch_params",
        dag=dag,
    )
    mock_response = Response()
    mock_response._content = b'{"id": 1}'
    patched_hook = mocker.patch.object(HttpHook, "run", return_value=mock_response)

    op.submit_batch()

    assert op.timeout_minutes == timeout_minutes
    assert op.poll_period_sec == poll_period_sec
    assert op.verify_in == verify_in
    assert op.http_conn_id_livy == http_conn_id_livy
    assert op.http_conn_id_spark == http_conn_id_spark
    assert op.http_conn_id_yarn == http_conn_id_yarn
    expected_json = json.loads(
        """{
      "proxyUser": "proxy_user",
      "file": "file",
      "className": "class_name",
      "args": [
        "arg2",
        "arg1"
      ],
      "pyFiles": [
        "py_file1",
        "py_file2"
      ],
      "jars": [
        "jar1",
        "jar2"
      ],
      "files": [
        "file1",
        "file2"
      ],
      "driverMemory": "driver_memory",
      "driverCores": 1,
      "executorMemory": "executor_memory",
      "executorCores": 2,
      "numExecutors": 3,
      "archives": [
        "archive1",
        "archive2"
      ],
      "name": "name",
      "queue": "queue",
      "conf": {
        "key1": "val1",
        "key2": 2
      }
    }"""
    )
    actual_args, actual_kwargs = patched_hook._call_matcher(patched_hook.call_args)
    actual_json = find_json_in_args(actual_args, actual_kwargs)
    if actual_json is None:
        raise AssertionError(
            f"Can not find JSON in HttpHook args.\n"
            f"Args:\n{actual_args}\n"
            f"KWArgs (JSON should be under 'data' key):\n{actual_kwargs}"
        )
    else:
        diff = DeepDiff(actual_json, expected_json, ignore_order=True)
        if diff:
            print(f"\nDifference:\n{json.dumps(diff, indent=2)}")
        assert not diff


@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_submit_batch_bad_response_codes(dag, mocker, code):
    op = LivyBatchOperator(
        task_id=f"test_submit_batch_bad_response_codes_{code}", dag=dag
    )
    http_response = mock_http_calls(
        code, content=b"Error content", reason="Good reason"
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        op.submit_batch()
    print(
        f"\n\nImitated the {code} error response when submitting a batch, "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_submit_batch_malformed_json(dag, mocker):
    op = LivyBatchOperator(task_id="test_submit_batch_malformed_json", dag=dag)
    http_response = mock_http_calls(201, content=b'{"id":{}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as bre:
        op.submit_batch()
    print(
        f"\n\nImitated malformed JSON response when submitting a batch, "
        f"got the expected exception:\n<{bre.value}>"
    )


def test_submit_batch_string_id(dag, mocker):
    op = LivyBatchOperator(task_id="test_submit_batch_string_id", dag=dag)
    http_response = mock_http_calls(201, content=b'{"id":"unexpectedly, a string!"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        op.submit_batch()
    print(
        f"\n\nImitated server returning a string for a batch ID, "
        f"got the expected exception:\n<{ae.value}>"
    )
