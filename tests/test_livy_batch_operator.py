from airflow import AirflowException
from airflow.exceptions import AirflowBadRequest
from airflow.hooks.http_hook import HttpHook
from pytest import raises, mark

from airflow_home.plugins import LivyBatchOperator
from tests.helpers import mock_http_response


def test_jinja(dag):
    op = LivyBatchOperator(
        name="test_jinja_{{ run_id }}",
        arguments=[
            "{{ run_id|replace(':', '-') }}",
            "prefix {{ custom_param }} postfix",
        ],
        task_id="test_jinja",
        dag=dag,
    )
    op.render_template_fields({"run_id": "hello:world", "custom_param": "custom value"})
    assert op.name == "test_jinja_hello:world"
    assert op.arguments[0] == "hello-world"
    assert op.arguments[1] == "prefix custom value postfix"


def test_invalid_verification(dag):
    with raises(AirflowException) as ae:
        LivyBatchOperator(
            task_id="test_invalid_verification", verify_in="invalid", dag=dag,
        )
    print(
        f"\n\nCreated a batch operator with invalid veification method, "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_submit_batch(dag, mocker):
    op = LivyBatchOperator(task_id="test_submit_batch", dag=dag)
    http_response = mock_http_response(
        201,
        content=b"""{
      "id": 123,
      "name": null,
      "owner": null,
      "proxyUser": null,
      "state": "starting",
      "appId": null,
      "appInfo": {
        "driverLogUrl": null,
        "sparkUiUrl": null
      },
      "log": [
        "stdout: ",
        "\\nstderr: ",
        "\\nYARN Diagnostics: "
      ]
    }""",
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    val = op.submit_batch()
    assert val == 123


@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_submit_batch_bad_response_codes(dag, mocker, code):
    op = LivyBatchOperator(task_id="test_submit_batch_bad_response_codes", dag=dag)
    http_response = mock_http_response(
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
    http_response = mock_http_response(201, content=b'{"id":{}', reason="Not found")
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as bre:
        op.submit_batch()
    print(
        f"\n\nImitated malformed JSON response when submitting a batch, "
        f"got the expected exception:\n<{bre.value}>"
    )


def test_submit_batch_string_id(dag, mocker):
    op = LivyBatchOperator(task_id="test_submit_batch_invalid_json", dag=dag)
    http_response = mock_http_response(201, content=b'{"id":"unexpectedly, a string!"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        op.submit_batch()
    print(
        f"\n\nImitated server returning a string for a batch ID, "
        f"got the expected exception:\n<{ae.value}>"
    )
