from airflow import AirflowException
from airflow.exceptions import AirflowBadRequest, AirflowSensorTimeout
from airflow.providers.http.hooks.http import HttpHook
from pytest import mark, raises

from airflow_home.plugins.airflow_livy.session import LivyStatementSensor
from tests.helpers import mock_http_calls


def test_statement_sensor(mocker):
    sen = LivyStatementSensor(
        session_id=123,
        statement_id=456,
        poke_interval=1,
        timeout=3,
        task_id="test_statement_sensor",
    )
    response = mock_http_calls(
        200,
        content=b"""
        {
            "id": 123,
            "state": "available",
            "output": {
                "status": "ok"
            }
        }""",
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=response)
    sen.execute({})


@mark.parametrize(
    "poke_interval,timeout", [(0, 0), (0, 1), (1, 0), (10, 0), (10, 1), (1000, 2)]
)
def test_statement_sensor_invalid_timings(poke_interval, timeout):
    print(
        f"\n\nRunning test_statement_sensor_invalid_timings with:\n"
        f"poke_interval={poke_interval}, timeout={timeout}"
    )
    with raises(AirflowException) as ae:
        LivyStatementSensor(
            session_id=123,
            statement_id=456,
            poke_interval=poke_interval,
            timeout=timeout,
            task_id="test_statement_sensor_invalid_timings",
        )
    print(
        f"Set up the statement sensor with invalid timings, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


def test_statement_sensor_timeout(mocker):
    sen = LivyStatementSensor(
        session_id=123,
        statement_id=456,
        poke_interval=1,
        timeout=3,
        task_id="test_statement_sensor_timeout",
    )
    http_response = mock_http_calls(200, content=b'{"id": 123, "state": "running"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowSensorTimeout) as te:
        sen.execute({})
    assert 2 <= http_response.send.call_count <= 4
    print(
        f"\n\nImitated period=1sec, timeout=2sec while waiting for statement "
        f"to complete, got the expected exception:\n<{te.value}>\n"
        f"API was polled {http_response.send.call_count} times."
    )


def test_statement_sensor_malformed_json(mocker):
    sen = LivyStatementSensor(
        session_id=123,
        statement_id=456,
        poke_interval=2,
        timeout=5,
        task_id="test_statement_sensor_malformed_json",
    )
    http_response = mock_http_calls(200, content=b'{"id": 123, "state: "running"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as ae:
        sen.execute({})
    print(
        f"Imitated malformed JSON response for statement sensor, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


def test_statement_sensor_malformed_status(mocker):
    sen = LivyStatementSensor(
        session_id=123,
        statement_id=456,
        poke_interval=2,
        timeout=5,
        task_id="test_statement_sensor_malformed_status",
    )
    http_response = mock_http_calls(
        200,
        content=b"""
        {
            "id": 123,
            "state": "available",
            "output": {
                "nostatus": "here"
            }
        }""",
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as ae:
        sen.execute({})
    print(
        f"Imitated invalid 'output' object for statement sensor, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


def test_statement_sensor_unknown_status(mocker):
    sen = LivyStatementSensor(
        session_id=123,
        statement_id=456,
        poke_interval=2,
        timeout=5,
        task_id="test_statement_sensor_unknown_status",
    )
    http_response = mock_http_calls(
        200,
        content=b"""
        {
            "id": 123,
            "state": "available",
            "output": {
                "status": "something"
            }
        }""",
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as ae:
        sen.execute({})
    print(
        f"Imitated unknown status for statement sensor, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


def test_statement_sensor_unknown_state(mocker):
    sen = LivyStatementSensor(
        session_id=123,
        statement_id=456,
        poke_interval=2,
        timeout=5,
        task_id="test_statement_sensor_unknown_state",
    )
    http_response = mock_http_calls(200, content=b'{"id": 123, "state": "IDK"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as ae:
        sen.execute({})
    print(
        f"Imitated unknown statement state for statement sensor, "
        f"got the expected exception:\n<{ae.value}>\n"
    )
