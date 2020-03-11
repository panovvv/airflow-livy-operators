import requests
import responses
from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from pytest import mark, raises

from airflow_home.plugins.airflow_livy.session import LivySessionOperator
from tests.helpers import MockedResponse
from tests.mock_sessions import STATEMENT_ID, mock_livy_session_responses


@responses.activate
def test_run_session_successfully(dag, mocker):
    st1 = LivySessionOperator.Statement(kind="spark", code="x = 1;")
    st2 = LivySessionOperator.Statement(kind="pyspark", code="print 'hi';")
    statements = [st1, st2]
    op = LivySessionOperator(
        statements=statements,
        spill_logs=False,
        task_id="test_run_session_successfully",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_session_logs")
    submit_statement_spy = mocker.spy(op, "submit_statement")
    mock_livy_session_responses(mocker)
    op.execute({})

    submit_statement_spy.call_count = len(statements)

    # spill_logs is False and session completed successfully, so we don't expect logs.
    spill_logs_spy.assert_not_called()
    op.spill_logs = True
    op.execute({})

    submit_statement_spy.call_count = len(statements) * 2
    # We set spill_logs to True this time, therefore expecting logs.
    spill_logs_spy.assert_called_once()


def test_run_session_error_before_session_created(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_error_before_session_created",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_session_logs")
    mocker.patch.object(
        BaseHook,
        "_get_connections_from_db",
        return_value=[Connection(host="HOST", port=123)],
    )
    with raises(requests.exceptions.ConnectionError) as ae:
        op.execute({})
    print(
        f"\n\nNo response from server was mocked, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # Even though we set spill_logs to True, Operator doesn't have a session_id yet.
    spill_logs_spy.assert_not_called()


@responses.activate
@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_run_session_error_during_status_probing(dag, mocker, code):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_error_during_status_probing",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_session_logs")
    mock_livy_session_responses(
        mocker,
        mock_get_session=[MockedResponse(code, body=f"Response from server:{code}")],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated {code} response from server during session creation probing , "
        f"got the expected exception:\n<{ae.value}>"
    )
    # spill_logs=True, and Operator had the session_id by the time error occured.
    spill_logs_spy.assert_called_once()
    op.spill_logs = False
    with raises(AirflowException):
        op.execute({})
    # spill_logs=False, but error occured and Operator had the session_id.
    assert spill_logs_spy.call_count == 2


@responses.activate
@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_run_session_error_when_submitting_statement(dag, mocker, code):
    st1 = LivySessionOperator.Statement(kind="spark", code="x = 1;")
    st2 = LivySessionOperator.Statement(kind="pyspark", code="print 'hi';")
    op = LivySessionOperator(
        statements=[st1, st2],
        spill_logs=True,
        task_id="test_run_session_error_when_submitting_statement",
        dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_session_logs")
    submit_statement_spy = mocker.spy(op, "submit_statement")
    mock_livy_session_responses(
        mocker,
        mock_post_statement=[
            MockedResponse(200, json_body={"id": STATEMENT_ID}),
            MockedResponse(code, body=f"Response from server:{code}"),
            MockedResponse(200, json_body={"no id here": "haha"}),
        ],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated {code} response from server during second statement submission, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # spill_logs=True, and Operator had the session_id by the time error occured.
    spill_logs_spy.assert_called_once()
    assert submit_statement_spy.call_count == 2
    op.spill_logs = False
    with raises(AirflowException):
        op.execute({})
    print(
        f"\n\nImitated {code} response from server during first statement submission, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # spill_logs=False, but error occured and Operator had the session_id.
    assert spill_logs_spy.call_count == 2
    assert submit_statement_spy.call_count == 3


@responses.activate
def test_run_session_logs_less_pages_than_page_size(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_logs_less_pages_than_page_size",
        dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_session_responses(mocker, log_lines=7)
    op.execute({})
    fetch_log_page_spy.assert_called_once()


@responses.activate
def test_run_session_logs_one_page_size(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_logs_one_page_size",
        dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_session_responses(mocker, log_lines=100)
    op.execute({})
    fetch_log_page_spy.assert_called_once()


@responses.activate
def test_run_session_logs_multiple_of_page_size(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_logs_multiple_of_page_size",
        dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_session_responses(mocker, log_lines=300)
    op.execute({})
    assert fetch_log_page_spy.call_count == 3


@responses.activate
def test_run_session_logs_greater_than_page_size(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_logs_greater_than_page_size",
        dag=dag,
    )
    fetch_log_page_spy = mocker.spy(op, "fetch_log_page")
    mock_livy_session_responses(mocker, log_lines=321)
    op.execute({})
    assert fetch_log_page_spy.call_count == 4


@responses.activate
def test_run_session_logs_malformed_json(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_logs_greater_than_page_size",
        dag=dag,
    )
    mock_livy_session_responses(mocker, log_override_response='{"invalid":json]}')
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated malformed response when calling /logs , "
        f"got the expected exception:\n<{ae.value}>"
    )


@responses.activate
def test_run_session_logs_missing_attrs_in_json(dag, mocker):
    op = LivySessionOperator(
        statements=[],
        spill_logs=True,
        task_id="test_run_session_logs_missing_attrs_in_json",
        dag=dag,
    )
    mock_livy_session_responses(mocker, log_override_response='{"id": 1, "from": 2}')
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nImitated missing attributes when calling /logs , "
        f"got the expected exception:\n<{ae.value}>"
    )
