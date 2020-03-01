import json
from collections import Iterable
from typing import Mapping
from unittest.mock import Mock

import responses
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from requests import Response

HOST = "host"
PORT = 1234
URI = f"http://{HOST}:{PORT}"
BATCH_ID = 99
APP_ID = "mock_app_id"


def mock_http_calls(status_code, content=None, reason=None):
    http_response = Response()
    http_response.status_code = status_code
    http_response.headers = {}
    if content is not None:
        if isinstance(content, (bytes, bytearray)):
            http_response._content = content
        else:
            http_response._content = content.encode("ascii")
    if reason is not None:
        http_response.reason = reason
    mocked_response = Mock()
    mocked_response.send.return_value = http_response
    return mocked_response


def mock_livy_batch_responses(
    mocker: Mock,
    mock_create: Mapping = None,
    mock_get: Mapping = None,
    mock_spark: Mapping = None,
    mock_yarn: Mapping = None,
    log_lines=1,
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


def _mock_create_response(response: Mapping = None):
    if response is None:
        response = {201: {"id": BATCH_ID}}
    for code, data in response.items():
        body, json_body = _get_http_data(data)
        responses.add(
            responses.POST, f"{URI}/batches", status=code, body=body, json=json_body
        )


def _mock_get_response(response: Mapping = None):
    if response is None:
        response = {200: {"state": "success", "appId": APP_ID}}
    for code, data in response.items():
        body, json_body = _get_http_data(data)
        responses.add(
            responses.GET,
            f"{URI}/batches/{BATCH_ID}",
            status=code,
            body=body,
            json=json_body,
        )


def _mock_spark_response(response: Mapping = None):
    if response is None:
        response = {
            200: [
                {"jobId": 1, "status": "SUCCEEDED"},
                {"jobId": 2, "status": "SUCCEEDED"},
            ]
        }
    for code, data in response.items():
        body, json_body = _get_http_data(data)
        responses.add(
            responses.GET,
            f"{URI}/api/v1/applications/{APP_ID}/jobs",
            status=code,
            body=body,
            json=json_body,
        )


def _mock_yarn_response(response: Mapping = None):
    if response is None:
        response = {200: {"app": {"finalStatus": "SUCCEEDED"}}}
    for code, data in response.items():
        body, json_body = _get_http_data(data)
        responses.add(
            responses.GET,
            f"{URI}/ws/v1/cluster/apps/{APP_ID}",
            status=code,
            body=body,
            json=json_body,
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
        log_mocker = LogMocker(log_lines)
        responses.add_callback(
            responses.GET,
            f"{URI}/batches/{BATCH_ID}/log",
            callback=log_mocker.mock_logs,
            content_type="application/json",
            match_querystring=False,
        )


class LogMocker:
    def __init__(self, log_lines: int) -> None:
        if not log_lines:
            log_lines = 2
        self.lines = log_lines

    def mock_logs(self, request):
        req_line = int(request.params[f"/batches/{BATCH_ID}/log?from"])
        req_size = int(request.params["size"])
        line_from = req_line if req_line < self.lines else self.lines - 1
        req_to = line_from + req_size
        line_to = req_to if req_to < self.lines else self.lines
        page = {
            "id": BATCH_ID,
            "from": line_from,
            "total": self.lines,
            "log": [f"line {n}" for n in range(line_from, line_to)],
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(page)


def _mock_delete_response(status=None):
    if status is None:
        status = 200
    responses.add(responses.DELETE, f"{URI}/batches/{BATCH_ID}", status=status)


def _get_http_data(data):
    return (
        (None, data)
        if isinstance(data, Mapping) or isinstance(data, Iterable)
        else (data, None)
    )
