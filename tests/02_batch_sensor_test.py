from airflow import AirflowException
from airflow.exceptions import AirflowBadRequest, AirflowSensorTimeout
from airflow.providers.http.hooks.http import HttpHook
from pytest import mark, raises

from airflow_home.plugins.airflow_livy.batch import LivyBatchSensor
from tests.helpers import mock_http_calls


def test_batch_sensor(mocker):
    sen = LivyBatchSensor(batch_id=2, task_id="test_batch_sensor")
    http_response = mock_http_calls(
        200,
        content=b'{"id": 2, "state": "success"}',
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    sen.execute({})


def test_batch_sensor_timeout(mocker):
    sen = LivyBatchSensor(
        batch_id=2,
        task_id="test_batch_sensor_timeout",
        poke_interval=1,
        timeout=2,
    )
    http_response = mock_http_calls(200, content=b'{"id": 2, "state": "starting"}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowSensorTimeout) as te:
        sen.execute({})
    assert 2 <= http_response.send.call_count <= 4
    print(
        f"\n\nImitated period=1sec, timeout=2sec while waiting for batch to finish, "
        f"got the expected exception:\n<{te.value}>\n"
        f"API was polled {http_response.send.call_count} times."
    )


@mark.parametrize(
    "poke_interval,timeout", [(0, 0), (0, 1), (1, 0), (10, 0), (10, 1), (1000, 2)]
)
def test_batch_sensor_invalid_timings(poke_interval, timeout):
    print(
        f"\n\nRunning test_batch_sensor_invalid_timings with:\n"
        f"poke_interval={poke_interval}, timeout={timeout}"
    )
    with raises(AirflowException) as ae:
        LivyBatchSensor(
            batch_id=2,
            task_id="test_batch_sensor_timeout_invalid",
            poke_interval=poke_interval,
            timeout=timeout,
        )
    print(
        f"Set up the batch sensor with invalid timings, "
        f"got the expected exception:\n<{ae.value}>\n"
    )


@mark.parametrize(
    "state",
    [
        "not_started",
        "starting",
        "recovering",
        "idle",
        "running",
        "busy",
        "shutting_down",
    ],
)
def test_batch_sensor_valid_states(mocker, state):
    sen = LivyBatchSensor(batch_id=2, task_id="test_batch_sensor_valid_states")
    http_response = mock_http_calls(200, content=f'{{"id": 2, "state": "{state}"}}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    assert not sen.poke({})


@mark.parametrize(
    "state",
    ["error", "dead", "killed", "asdsas", 123, -1],
)
def test_batch_sensor_invalid_states(dag, mocker, state):
    sen = LivyBatchSensor(batch_id=2, task_id="test_batch_sensor_invalid_states")
    http_response = mock_http_calls(200, content=f'{{"id": 2, "state": "{state}"}}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        sen.poke({})
    print(
        f"\n\nImitated invalid state while waiting for batch to finish, "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_batch_sensor_malformed_json(mocker):
    sen = LivyBatchSensor(batch_id=2, task_id="test_batch_sensor_malformed_json")
    http_response = mock_http_calls(200, content='{{"id": 2, "state": }}')
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowBadRequest) as bre:
        sen.poke({})
    print(
        f"\n\nImitated invalid state while waiting for batch to finish, "
        f"got the expected exception:\n<{bre.value}>"
    )


@mark.parametrize("code", [404, 403, 500, 503, 504])
def test_batch_sensor_bad_response_codes(mocker, code):
    sen = LivyBatchSensor(batch_id=2, task_id="test_batch_sensor_malformed_json")
    http_response = mock_http_calls(
        code, content=b"Error content", reason="Good reason"
    )
    mocker.patch.object(HttpHook, "get_conn", return_value=http_response)
    with raises(AirflowException) as ae:
        sen.poke({})
    print(
        f"\n\nImitated the {code} error response when submitting a batch, "
        f"got the expected exception:\n<{ae.value}>"
    )
