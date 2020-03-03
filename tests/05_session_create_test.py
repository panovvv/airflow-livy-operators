import json

from airflow import AirflowException
from airflow.exceptions import AirflowBadRequest
from airflow.hooks.http_hook import HttpHook
from deepdiff import DeepDiff
from pytest import mark, raises
from requests import Response

from plugins.airflow_livy import LivySessionOperator
from tests.helpers import find_json_in_args, mock_http_calls


def test_create_session_get_id(dag, mocker):
    op = LivySessionOperator(
        statements=[], task_id="test_create_session_get_id", dag=dag,
    )
    http_response = mock_http_calls(201, content=b'{"id": 456}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    op.create_session()
    assert op.session_id == 456


def test_create_session_params(dag, mocker):
    heartbeat_timeout = 9
    session_start_timeout_sec = 11
    session_start_poll_period_sec = 22
    statemt_timeout_minutes = 33
    statemt_poll_period_sec = 44
    http_conn_id = "foo"
    spill_logs = True
    st1 = LivySessionOperator.Statement(kind="spark", code="x=1")
    st2 = LivySessionOperator.Statement(kind="sparkr", code="print hi")
    op = LivySessionOperator(
        statements=[st1, st2],
        kind="pyspark",
        proxy_user="proxy_user",
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
        heartbeat_timeout=heartbeat_timeout,
        session_start_timeout_sec=session_start_timeout_sec,
        session_start_poll_period_sec=session_start_poll_period_sec,
        statemt_timeout_minutes=statemt_timeout_minutes,
        statemt_poll_period_sec=statemt_poll_period_sec,
        http_conn_id=http_conn_id,
        spill_logs=spill_logs,
        task_id="test_create_session_params",
        dag=dag,
    )
    mock_response = Response()
    mock_response._content = b'{"id": 1}'
    patched_hook = mocker.patch.object(HttpHook, "run", return_value=mock_response)

    op.create_session()

    assert op.statements[0] == st1
    assert op.statements[1] == st2
    assert op.heartbeat_timeout == heartbeat_timeout
    assert op.session_start_timeout_sec == session_start_timeout_sec
    assert op.session_start_poll_period_sec == session_start_poll_period_sec
    assert op.statemt_timeout_minutes == statemt_timeout_minutes
    assert op.statemt_poll_period_sec == statemt_poll_period_sec
    assert op.http_conn_id == http_conn_id
    assert op.spill_logs == spill_logs
    expected_json = json.loads(
        """{
          "kind": "pyspark",
          "proxyUser": "proxy_user",
          "jars": [
            "jar1",
            "jar2"
          ],
          "pyFiles": [
            "py_file1",
            "py_file2"
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
          "queue": "queue",
          "name": "name",
          "conf": {
            "key1": "val1",
            "key2": 2
          },
          "heartbeatTimeoutInSecond": 9
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
def test_create_session_bad_response_codes(dag, mocker, code):
    op = LivySessionOperator(
        statements=[], task_id="test_create_session_bad_response_codes", dag=dag
    )
    http_response = mock_http_calls(
        code, content=b"Error content", reason="Good reason"
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        op.create_session()
    print(
        f"\n\nImitated the {code} error response when creating a session, "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_create_session_malformed_json(dag, mocker):
    op = LivySessionOperator(
        statements=[], task_id="test_create_session_malformed_json", dag=dag
    )
    http_response = mock_http_calls(201, content=b'{"id":{}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as bre:
        op.create_session()
    print(
        f"\n\nImitated malformed JSON response when creating a session, "
        f"got the expected exception:\n<{bre.value}>"
    )


def test_create_session_string_id(dag, mocker):
    op = LivySessionOperator(
        statements=[], task_id="test_create_session_string_id", dag=dag
    )
    http_response = mock_http_calls(201, content=b'{"id":"unexpectedly, a string!"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        op.create_session()
    print(
        f"\n\nImitated server returning a string for a session ID, "
        f"got the expected exception:\n<{ae.value}>"
    )
