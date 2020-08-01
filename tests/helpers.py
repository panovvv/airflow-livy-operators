import json
from collections import Iterable
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, Mapping
from unittest.mock import Mock

from requests import Response


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


def find_json_in_args(args_list: Iterable, kwargs_map: Mapping):
    if kwargs_map.get("data"):
        return json.loads(kwargs_map["data"])
    else:
        for arg in args_list:
            try:
                return json.loads(arg)
            except (JSONDecodeError, TypeError):
                pass


class LogMocker:
    TYPES = ["batches", "sessions"]

    def __init__(self, log_lines: int, job_type: str, job_id: int) -> None:
        if job_type not in self.TYPES:
            raise Exception(f"Can't mock logs for {job_type}. Supported: {self.TYPES}")
        self.job_type = job_type
        self.lines = log_lines
        self.job_id = job_id

    def mock_logs(self, request):
        req_line = int(request.params["from"])
        req_size = int(request.params["size"])
        line_from = req_line if req_line < self.lines else self.lines - 1
        req_to = line_from + req_size
        line_to = req_to if req_to < self.lines else self.lines
        page = {
            "id": self.job_id,
            "from": line_from,
            "total": self.lines,
            "log": [f"--> Log line {n} <--" for n in range(line_from, line_to)],
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(page)


@dataclass
class MockedResponse:
    status: int
    body: Any = None
    json_body: Any = None
