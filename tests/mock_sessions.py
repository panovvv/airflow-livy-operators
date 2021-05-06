from typing import Iterable
from unittest.mock import Mock

import responses
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook

from tests.helpers import LogMocker, MockedResponse

HOST = "host"
PORT = 1234
URI = f"http://{HOST}:{PORT}"
SESSION_ID = 77
STATEMENT_ID = 66


def mock_livy_session_responses(
    mocker: Mock,
    mock_create: Iterable[MockedResponse] = None,
    mock_get_session: Iterable[MockedResponse] = None,
    mock_post_statement: Iterable[MockedResponse] = None,
    mock_get_statement: Iterable[MockedResponse] = None,
    log_lines=5,
    log_override_response=None,
    mock_delete: int = None,
):
    _mock_create_response(mock_create)
    _mock_get_session_response(mock_get_session)
    _mock_post_statement_response(mock_post_statement)
    _mock_get_statement_response(mock_get_statement)
    _mock_log_response(log_override_response, log_lines)
    _mock_delete_response(mock_delete)
    mocker.patch.object(
        HttpHook,
        "get_connection",
        return_value=Connection(host=HOST, port=PORT),
    )


def _mock_create_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [MockedResponse(201, json_body={"id": SESSION_ID})]
    for resp in response_list:
        responses.add(
            responses.POST,
            f"{URI}/sessions",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_get_session_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [MockedResponse(200, json_body={"state": "idle"})]
    for resp in response_list:
        responses.add(
            responses.GET,
            f"{URI}/sessions/{SESSION_ID}/state",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_post_statement_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [MockedResponse(200, json_body={"id": STATEMENT_ID})]
    for resp in response_list:
        responses.add(
            responses.POST,
            f"{URI}/sessions/{SESSION_ID}/statements",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_get_statement_response(response_list: Iterable[MockedResponse] = None):
    if response_list is None:
        response_list = [
            MockedResponse(
                200, json_body={"state": "available", "output": {"status": "ok"}}
            )
        ]
    for resp in response_list:
        responses.add(
            responses.GET,
            f"{URI}/sessions/{SESSION_ID}/statements/{STATEMENT_ID}",
            status=resp.status,
            body=resp.body,
            json=resp.json_body,
        )


def _mock_log_response(log_override_response, log_lines: int):
    if log_override_response:
        responses.add(
            responses.GET,
            f"{URI}/sessions/{SESSION_ID}/log",
            match_querystring=False,
            status=200,
            body=log_override_response,
        )
    else:
        log_mocker = LogMocker(log_lines, "sessions", SESSION_ID)
        responses.add_callback(
            responses.GET,
            f"{URI}/sessions/{SESSION_ID}/log",
            callback=log_mocker.mock_logs,
            content_type="application/json",
            match_querystring=False,
        )


def _mock_delete_response(status=None):
    if status is None:
        status = 200
    responses.add(responses.DELETE, f"{URI}/sessions/{SESSION_ID}", status=status)
