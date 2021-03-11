import requests
import responses
from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook
from pytest import mark, raises

from airflow_home.plugins.airflow_livy.batch import LivyBatchOperator
from tests.helpers import MockedResponse
from tests.mock_batches import mock_livy_batch_responses


@responses.activate
def test_run_batch_successfully(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=False, task_id="test_run_batch_successfully", dag=dag
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    submit_batch_spy = mocker.spy(op, "submit_batch")
    mock_livy_batch_responses(mocker)
    op.execute({})

    submit_batch_spy.assert_called_once()
    # spill_logs is False and batch completed successfully, so we don't expect logs.
    spill_logs_spy.assert_not_called()
    op.spill_logs = True
    op.execute({})

    # We set spill_logs to True this time, therefore expecting logs.
    spill_logs_spy.assert_called_once()


def test_run_batch_error_before_batch_created(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True, task_id="test_run_batch_error_before_batch_created", dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mocker.patch.object(
        HttpHook, "get_connection", return_value=Connection(host="HOST", port=123),
    )
    with raises(requests.exceptions.ConnectionError) as ae:
        op.execute({})
    print(
        f"\n\nNo response from server was mocked, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # Even though we set spill_logs to True, Operator doesn't have a batch_id yet.
    spill_logs_spy.assert_not_called()


@responses.activate
@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_run_batch_error_during_status_probing(dag, mocker, code):
    op = LivyBatchOperator(
        spill_logs=True,
        task_id=f"test_run_batch_error_during_status_probing_{code}",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_livy_batch_responses(
        mocker, mock_get=[MockedResponse(code, body=f"Response from server:{code}")]
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated {code} response from server during batch status probing , "
        f"got the expected exception:\n<{ae.value}>"
    )
    # spill_logs=True, and Operator had the batch_id by the time error occured.
    spill_logs_spy.assert_called_once()
    op.spill_logs = False
    with raises(AirflowException):
        op.execute({})
    # spill_logs=False, but error occured and Operator had the batch_id.
    assert spill_logs_spy.call_count == 2


@responses.activate
def test_run_batch_verify_in_spark(dag, mocker):
    op = LivyBatchOperator(
        verify_in="spark",
        spill_logs=False,
        task_id="test_run_batch_verify_in_spark",
        dag=dag,
    )
    spark_checker_spy = mocker.spy(op, "check_spark_app_status")
    mock_livy_batch_responses(mocker)
    op.execute({})
    spark_checker_spy.assert_called_once()


@responses.activate
def test_run_batch_no_appid(dag, mocker):
    op = LivyBatchOperator(
        verify_in="spark", spill_logs=False, task_id="test_run_batch_no_appid", dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_livy_batch_responses(
        mocker,
        mock_get=[MockedResponse(200, json_body={"state": "success", "appId": None})],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated null Spark appId, then checked status via Spark REST API, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # spill_logs=True, and Operator had the batch_id by the time error occured.
    spill_logs_spy.assert_called_once()

    mock_livy_batch_responses(
        mocker,
        mock_get=[
            MockedResponse(200, json_body={"state": "success", "noAppId": "here"})
        ],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated no key for Spark appId, then checked status via Spark REST API, "
        f"got the expected exception:\n<{ae.value}>"
    )
    assert spill_logs_spy.call_count == 2


@responses.activate
def test_run_batch_verify_in_spark_failed(dag, mocker):
    op = LivyBatchOperator(
        verify_in="spark",
        spill_logs=False,
        task_id="test_run_batch_verify_in_spark_failed",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_livy_batch_responses(
        mocker,
        mock_spark=[
            MockedResponse(
                200,
                json_body=[
                    {"jobId": 1, "status": "SUCCEEDED"},
                    {"jobId": 2, "status": "FAILED"},
                ],
            )
        ],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated failed Spark job, then checked status via Spark REST API, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # spill_logs=True, and Operator had the batch_id by the time error occured.
    spill_logs_spy.assert_called_once()


@responses.activate
def test_run_batch_verify_in_spark_garbled(dag, mocker):
    op = LivyBatchOperator(
        verify_in="spark",
        spill_logs=False,
        task_id="test_run_batch_verify_in_spark_garbled",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_livy_batch_responses(
        mocker, mock_spark=[MockedResponse(200, json_body={"unparseable": "obj"})],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated garbled output from Spark REST API, "
        f"got the expected exception:\n<{ae.value}>"
    )
    spill_logs_spy.assert_called_once()


@responses.activate
def test_run_batch_verify_in_yarn(dag, mocker):
    op = LivyBatchOperator(
        verify_in="yarn",
        spill_logs=False,
        task_id="test_run_batch_verify_in_yarn",
        dag=dag,
    )
    yarn_checker_spy = mocker.spy(op, "check_yarn_app_status")
    mock_livy_batch_responses(mocker)
    op.execute({})
    yarn_checker_spy.assert_called_once()


@responses.activate
def test_run_batch_verify_in_yarn_garbled_response(dag, mocker):
    op = LivyBatchOperator(
        verify_in="yarn",
        spill_logs=False,
        task_id="test_run_batch_verify_in_yarn_garbled_response",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_livy_batch_responses(
        mocker,
        mock_yarn=[MockedResponse(200, body="<!DOCTYPE html><html>notjson</html>")],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated garbled output from YARN REST API, "
        f"got the expected exception:\n<{ae.value}>"
    )
    spill_logs_spy.assert_called_once()


@responses.activate
def test_run_batch_verify_in_yarn_failed(dag, mocker):
    op = LivyBatchOperator(
        verify_in="yarn",
        spill_logs=False,
        task_id="test_run_batch_verify_in_yarn_failed",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_livy_batch_responses(
        mocker,
        mock_yarn=[MockedResponse(200, json_body={"app": {"finalStatus": "NOTGOOD"}})],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated failed status from YARN REST API, "
        f"got the expected exception:\n<{ae.value}>"
    )
    spill_logs_spy.assert_called_once()


@responses.activate
def test_run_batch_logs_less_pages_than_page_size(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True,
        task_id="test_run_batch_logs_less_pages_than_page_size",
        dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_batch_responses(mocker, log_lines=7)
    op.execute({})
    fetch_log_page_spy.assert_called_once()


@responses.activate
def test_run_batch_logs_one_page_size(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True, task_id="test_run_batch_logs_one_page_size", dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_batch_responses(mocker, log_lines=100)
    op.execute({})
    fetch_log_page_spy.assert_called_once()


@responses.activate
def test_run_batch_logs_multiple_of_page_size(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True, task_id="test_run_batch_logs_multiple_of_page_size", dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_batch_responses(mocker, log_lines=300)
    op.execute({})
    assert fetch_log_page_spy.call_count == 3


@responses.activate
def test_run_batch_logs_greater_than_page_size(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True, task_id="test_run_batch_logs_greater_than_page_size", dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_batch_responses(mocker, log_lines=321)
    op.execute({})
    assert fetch_log_page_spy.call_count == 4


@responses.activate
def test_run_batch_logs_malformed_json(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True, task_id="test_run_batch_logs_malformed_json", dag=dag,
    )
    mock_livy_batch_responses(mocker, log_override_response='{"invalid":json]}')
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated malformed response when calling /logs , "
        f"got the expected exception:\n<{ae.value}>"
    )


@responses.activate
def test_run_batch_logs_missing_attrs_in_json(dag, mocker):
    op = LivyBatchOperator(
        spill_logs=True, task_id="test_run_batch_logs_missing_attrs_in_json", dag=dag,
    )
    mock_livy_batch_responses(mocker, log_override_response='{"id": 1, "from": 2}')
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated missing attributes when calling /logs , "
        f"got the expected exception:\n<{ae.value}>"
    )
