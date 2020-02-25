from unittest.mock import Mock

from requests import Response


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
