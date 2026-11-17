import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from conda_oci_mirror.oras import get_oras_client, repository_of


@pytest.mark.parametrize(
    "url, repository",
    [
        ("https://ghcr.io/v2/org/noarch/demo/tags/list", "ghcr.io/v2/org/noarch/demo"),
        ("https://ghcr.io/v2/org/demo/manifests/1.0-0", "ghcr.io/v2/org/demo"),
        ("https://ghcr.io/v2/org/demo/blobs/sha256:abc", "ghcr.io/v2/org/demo"),
        ("https://ghcr.io/v2/org/demo/blobs/uploads/", "ghcr.io/v2/org/demo"),
        ("https://ghcr.io/v2/org/demo/blobs/uploads/1?digest=x", "ghcr.io/v2/org/demo"),
        # A package can be called blobs or tags.
        ("https://ghcr.io/v2/org/blobs/blobs/sha256:abc", "ghcr.io/v2/org/blobs"),
        ("https://ghcr.io/v2/org/tags/tags/list", "ghcr.io/v2/org/tags"),
        ("https://ghcr.io/token", "ghcr.io"),
    ],
)
def test_repository_of(url, repository):
    assert repository_of(url) == repository


@pytest.fixture
def scoped_registry():
    """A registry whose bearer tokens are each valid for one repository only."""
    state = {"requests": []}
    basic = "Basic " + base64.b64encode(b"alice:secret").decode()

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body=b"", headers=None):
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlsplit(self.path)
            state["requests"].append(path.path)
            host = f"127.0.0.1:{self.server.server_port}"
            if path.path == "/token":
                if self.headers.get("Authorization") != basic:
                    return self.reply(403)
                scope = parse_qs(path.query)["scope"][0]
                body = json.dumps({"token": f"token-for-{scope}"}).encode()
                return self.reply(200, body)
            repository = path.path[len("/v2/") : -len("/tags/list")]
            scope = f"repository:{repository}:pull"
            if self.headers.get("Authorization") != f"Bearer token-for-{scope}":
                # Like registry:2, a token for another repository is answered with 401.
                return self.reply(
                    401,
                    headers={
                        "Www-Authenticate": f'Bearer realm="http://{host}/token",'
                        f'service="{host}",scope="{scope}"'
                    },
                )
            self.reply(200, json.dumps({"tags": [repository]}).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state["registry"] = f"127.0.0.1:{server.server_port}"
    yield state
    server.shutdown()


def test_each_repository_gets_and_keeps_its_own_token(scoped_registry, monkeypatch):
    monkeypatch.setenv("ORAS_USER", "alice")
    monkeypatch.setenv("ORAS_PASS", "secret")
    registry = scoped_registry["registry"]
    client = get_oras_client(f"http://{registry}/org")

    assert client.get_tags(f"{registry}/org/first") == ["org/first"]
    # A token scoped for org/first does not work for org/second: a new one is requested.
    assert client.get_tags(f"{registry}/org/second") == ["org/second"]
    # Back to org/first: its token is still there, no new token request.
    assert client.get_tags(f"{registry}/org/first") == ["org/first"]
    assert scoped_registry["requests"] == [
        "/v2/org/first/tags/list",
        "/token",
        "/v2/org/first/tags/list",
        "/v2/org/second/tags/list",
        "/token",
        "/v2/org/second/tags/list",
        "/v2/org/first/tags/list",
    ]
    assert client.headers["Authorization"].startswith("Basic ")
