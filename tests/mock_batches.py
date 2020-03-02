from typing import Iterable
from unittest.mock import Mock

import responses
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection

from tests.helpers import LogMocker, MockedResponse

HOST = "host"
PORT = 1234
URI = f"http://{HOST}:{PORT}"
BATCH_ID = 99
APP_ID = "mock_app_id"


def mock_livy_batch_responses(
    mocker: Mock,
    mock_create: Iterable[MockedResponse] = None,
    mock_get: Iterable[MockedResponse] = None,
    mock_spark: Iterable[MockedResponse] = None,
    mock_yarn: Iterable[MockedResponse] = None,
    log_lines=5,
    log_override_response=None,
    mock_delete: int = None,
):
    _mock_create_response(mock_create)
    _mock_get_response(mock_get)
    _mock_spark_response(mock_spark)
    _mock_yarn_response(mock_yarn)
    _mock_log_response(log_override_response, log_lines)
    _mock_delete_response(mock_delete)
    mocker.patch.object(
        BaseHook,
        "_get_connections_from_db",
        return_value=[Connection(host=HOST, port=PORT)],
    )


def _mock_create_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [MockedResponse(201, json_body={"id": BATCH_ID})]
    for resp in response_list:
        responses.add(
            responses.POST,
            f"{URI}/batches",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_get_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [
            MockedResponse(200, json_body={"state": "success", "appId": APP_ID})
        ]
    for resp in response_list:
        responses.add(
            responses.GET,
            f"{URI}/batches/{BATCH_ID}",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_spark_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [
            MockedResponse(
                200,
                json_body=[
                    {"jobId": 1, "status": "SUCCEEDED"},
                    {"jobId": 2, "status": "SUCCEEDED"},
                ],
            )
        ]
    for resp in response_list:
        responses.add(
            responses.GET,
            f"{URI}/api/v1/applications/{APP_ID}/jobs",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_yarn_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [
            MockedResponse(200, json_body={"app": {"finalStatus": "SUCCEEDED"}})
        ]
    for resp in response_list:
        responses.add(
            responses.GET,
            f"{URI}/ws/v1/cluster/apps/{APP_ID}",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_log_response(log_override_response, log_lines: int):
    if log_override_response:
        responses.add(
            responses.GET,
            f"{URI}/batches/{BATCH_ID}/log",
            match_querystring=False,
            status=200,
            body=log_override_response,
        )
    else:
        log_mocker = LogMocker(log_lines, "batches", BATCH_ID)
        responses.add_callback(
            responses.GET,
            f"{URI}/batches/{BATCH_ID}/log",
            callback=log_mocker.mock_logs,
            content_type="application/json",
            match_querystring=False,
        )


def _mock_delete_response(status=None):
    if status is None:
        status = 200
    responses.add(responses.DELETE, f"{URI}/batches/{BATCH_ID}", status=status)
