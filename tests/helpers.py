from unittest.mock import Mock

import responses
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from requests import Response

HOST = "host"
PORT = 1234
URI = f"http://{HOST}:{PORT}"


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
    mocker, batch_id, get_code, get_resp,
):
    # Makes no sense to fail batch submission - it's all tested in 01_batch_op_submit.py
    responses.add(responses.POST, f"{URI}/batches", status=201, json={"id": batch_id})
    responses.add(
        responses.GET, f"{URI}/batches/{batch_id}", status=get_code, json=get_resp
    )
    responses.add(
        responses.GET,
        f"{URI}/batches/{batch_id}/log?from=0&size=100",
        status=200,
        json={"id": batch_id, "from": 0, "total": 2, "log": ["line 1", "line 2"]},
    )
    responses.add(
        responses.DELETE, f"{URI}/batches/{batch_id}", status=200,
    )
    mocker.patch.object(
        BaseHook,
        "_get_connections_from_db",
        return_value=[Connection(host=HOST, port=PORT)],
    )
