from airflow import AirflowException
from airflow.exceptions import AirflowBadRequest, AirflowSensorTimeout
from airflow.providers.http.hooks.http import HttpHook
from pytest import mark, raises

from airflow_home.plugins.airflow_livy.session import LivySessionCreationSensor
from tests.helpers import mock_http_calls


def test_session_creation_sensor(mocker):
    sen = LivySessionCreationSensor(
        session_id=123,
        poke_interval=1,
        timeout=3,
        task_id="test_session_creation_sensor",
    )
    response = mock_http_calls(200, content=b'{"id": 123, "state": "idle"}',)
    mocker.patch.object(HttpHook, "get_conn", return_value=response)
    sen.execute({})
    assert sen.session_id == 123


@mark.parametrize(
    "poke_interval,timeout", [(0, 0), (0, 1), (1, 0), (10, 0), (10, 1), (1000, 2)]
)
def test_session_creation_sensor_invalid_timings(poke_interval, timeout):
    print(
        f"\n\nRunning test_session_creation_sensor_invalid_timings with:\n"
        f"poke_interval={poke_interval}, timeout={timeout}"
    )
    with raises(AirflowException) as ae:
        LivySessionCreationSensor(
            session_id=123,
            poke_interval=poke_interval,
            timeout=timeout,
            task_id="test_session_creation_sensor_invalid_timings",
        )
    print(
        f"Set up the session creation sensor with invalid timings, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


def test_session_creation_sensor_timeout(mocker):
    sen = LivySessionCreationSensor(
        session_id=123,
        poke_interval=1,
        timeout=3,
        task_id="test_session_creation_sensor_timeout",
    )
    http_response = mock_http_calls(200, content=b'{"id": 123, "state": "starting"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowSensorTimeout) as te:
        sen.execute({})
    assert 2 <= http_response.send.call_count <= 4
    print(
        f"\n\nImitated period=1sec, timeout=2sec while waiting for session "
        f"to be created, got the expected exception:\n<{te.value}>\n"
        f"API was polled {http_response.send.call_count} times."
    )


def test_session_creation_sensor_malformed_json(mocker):
    sen = LivySessionCreationSensor(
        session_id=123,
        poke_interval=2,
        timeout=5,
        task_id="test_session_creation_sensor_malformed_json",
    )
    http_response = mock_http_calls(200, content=b'{"id": 123, "state: "starting"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as ae:
        sen.execute({})
    print(
        f"Imitated malformed JSON response for session creation sensor, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


def test_session_creation_sensor_unknown_state(mocker):
    sen = LivySessionCreationSensor(
        session_id=123,
        poke_interval=2,
        timeout=5,
        task_id="test_session_creation_sensor_malformed_json",
    )
    http_response = mock_http_calls(200, content=b'{"id": 123, "state": "IDK"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        sen.execute({})
    print(
        f"Imitated unknown session state for session creation sensor, "
        f"got the expected exception:\n<{ae.value}>\n"
    )
