import base64
import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from conda_oci_mirror import defaults
from conda_oci_mirror.oras import get_oras_client


@pytest.fixture
def token_registry():
    """A registry that, like GHCR, only accepts bearer tokens from its token endpoint."""
    state = {"token": "t1", "requests": [], "blobs": {}, "manifests": {}}
    basic = "Basic " + base64.b64encode(b"alice:secret").decode()

    class Handler(BaseHTTPRequestHandler):
        def record(self):
            state["requests"].append((self.command, self.path.split("?")[0]))

        def reply(self, status, body=b"", headers=None):
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def authorized(self):
            if self.headers.get("Authorization") == f"Bearer {state['token']}":
                return True
            host = f"127.0.0.1:{self.server.server_port}"
            self.reply(
                401,
                headers={
                    "Www-Authenticate": f'Bearer realm="http://{host}/token",'
                    f'service="{host}",scope="repository:test/shards:push,pull"'
                },
            )
            return False

        def body(self):
            return self.rfile.read(int(self.headers.get("Content-Length", 0)))

        def do_GET(self):
            self.record()
            if self.path.startswith("/token"):
                # Tokens are only issued for credentials, never for a bearer token.
                ok = self.headers.get("Authorization") == basic
                body = json.dumps({"token": state["token"]}).encode()
                return self.reply(200 if ok else 403, body if ok else b"")
            self.reply(404)

        def do_POST(self):
            self.record()
            if self.authorized():
                self.reply(202, headers={"Location": "/v2/test/shards/blobs/uploads/1"})

        def do_PUT(self):
            self.record()
            if not self.authorized():
                return
            data = self.body()
            if "/manifests/" in self.path:
                state["manifests"][self.path.rsplit("/", 1)[1]] = json.loads(data)
            else:
                digest = self.path.split("digest=")[1].replace("%3A", ":")
                assert digest == "sha256:" + hashlib.sha256(data).hexdigest()
                state["blobs"][digest] = data
            self.reply(201)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state["registry"] = f"127.0.0.1:{server.server_port}"
    yield state
    server.shutdown()


def test_shard_pushes_reuse_the_token_and_the_config(
    token_registry, tmp_path, monkeypatch
):
    monkeypatch.setenv("ORAS_USER", "alice")
    monkeypatch.setenv("ORAS_PASS", "secret")
    client = get_oras_client(f"http://{token_registry['registry']}/test")
    basic = client.headers["Authorization"]

    def push(name):
        shard = tmp_path / f"{name}.msgpack.zst"
        shard.write_bytes(name.encode())
        client.push_single_layer(
            f"{token_registry['registry']}/test/shards:{name}",
            str(shard),
            defaults.repodata_shard_media_type_v1,
            f"shards/{shard.name}",
        )

    push("first")
    # 401, token, then config POST+PUT, blob POST+PUT, manifest PUT.
    assert len(token_registry["requests"]) == 7
    for name in ("second", "third"):
        push(name)
    # Every further shard is exactly blob POST, blob PUT, manifest PUT.
    assert len(token_registry["requests"]) == 7 + 2 * 3

    # An expired token is renewed with the credentials, not the stale token.
    token_registry["token"] = "t2"
    push("fourth")
    assert token_registry["requests"][-5:] == [
        ("POST", "/v2/test/shards/blobs/uploads/"),
        ("GET", "/token"),
        ("POST", "/v2/test/shards/blobs/uploads/"),
        ("PUT", "/v2/test/shards/blobs/uploads/1"),
        ("PUT", "/v2/test/shards/manifests/fourth"),
    ]

    manifest = token_registry["manifests"]["fourth"]
    (layer,) = manifest["layers"]
    assert layer["mediaType"] == defaults.repodata_shard_media_type_v1
    assert layer["digest"] == "sha256:" + hashlib.sha256(b"fourth").hexdigest()
    assert layer["annotations"]["org.opencontainers.image.title"] == (
        "shards/fourth.msgpack.zst"
    )
    assert manifest["config"]["digest"] in token_registry["blobs"]
    # The bearer token never leaks into the client's shared headers.
    assert client.headers["Authorization"] == basic
