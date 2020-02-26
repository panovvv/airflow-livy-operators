import responses
from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from pytest import mark, raises

from airflow_home.plugins import LivyBatchOperator
from tests.helpers import mock_batch_responses

BATCH_ID = 99


@responses.activate
def test_run_batch_successfully(dag, mocker):
    op = LivyBatchOperator(
        task_id="test_run_batch_successfully", spill_logs=False, dag=dag
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_batch_responses(
        mocker, batch_id=BATCH_ID, get_code=200, get_resp={"state": "success"},
    )
    op.execute({})

    # We set spill_logs to False and batch completed successfully, so we don't expect logs.
    spill_logs_spy.assert_not_called()
    op.spill_logs = True
    op.execute({})

    # We set spill_logs to True this time, therefore expecting logs.
    spill_logs_spy.assert_called_once()


def test_run_batch_error_before_batch_created(dag, mocker):
    op = LivyBatchOperator(
        task_id="test_run_batch_error_before_batch_created", spill_logs=True, dag=dag,
    )
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mocker.patch.object(
        BaseHook,
        "_get_connections_from_db",
        return_value=[Connection(host="HOST", port=123)],
    )
    with raises(AirflowException) as ae:
        op.execute({})
    print(
        f"\n\nNo response from server was mocked, "
        f"got the expected exception:\n<{ae.value}>"
    )
    # Even though we set spill_logs to True, Operator doesn't have a batch_id yet.
    spill_logs_spy.assert_not_called()


@responses.activate
@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_run_batch_exception_during_status_probing(dag, mocker, code):
    op = LivyBatchOperator(task_id="test_run_batch", spill_logs=True, dag=dag)
    spill_logs_spy = mocker.spy(op, "spill_batch_logs")
    mock_batch_responses(
        mocker, batch_id=BATCH_ID, get_code=code, get_resp=None,
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
