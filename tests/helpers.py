from unittest.mock import Mock

import responses
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from requests import Response

HOST = "host"
PORT = 1234
URI = f"http://{HOST}:{PORT}"
BATCH_ID = 99
LOG_PAGE_SIZE = 100
APP_ID = "mock_app_id"


def mock_http_response(status_code, content=None, reason=None):
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


def mock_batch_responses(
    mocker,
    mock_create=True,
    mock_status=True,
    mock_spark=False,
    mock_yarn=False,
    mock_delete=True,
    log_pages=1,
):
    if mock_create:
        responses.add(
            responses.POST, f"{URI}/batches", status=201, json={"id": BATCH_ID}
        )
    if mock_status:
        responses.add(
            responses.GET,
            f"{URI}/batches/{BATCH_ID}",
            status=200,
            json={"state": "success", "appId": APP_ID},
        )
    if mock_spark:
        responses.add(
            responses.GET,
            f"{URI}/api/v1/applications/{APP_ID}/jobs",
            status=200,
            json=[
                {"jobId": 1, "status": "SUCCEEDED"},
                {"jobId": 2, "status": "SUCCEEDED"},
            ],
        )
    if log_pages:
        for page in range(log_pages):
            line_from = page * LOG_PAGE_SIZE
            line_to = (1 + page) * LOG_PAGE_SIZE
            responses.add(
                responses.GET,
                f"{URI}/batches/{BATCH_ID}/log?from={line_from}&size={line_to}",
                status=200,
                json={
                    "id": BATCH_ID,
                    "from": line_from,
                    "total": line_to,
                    "log": [f"line {n}" for n in range(line_from, line_to)],
                },
            )
    if mock_delete:
        responses.add(
            responses.DELETE, f"{URI}/batches/{BATCH_ID}", status=200,
        )

    mocker.patch.object(
        BaseHook,
        "_get_connections_from_db",
        return_value=[Connection(host=HOST, port=PORT)],
    )
