import base64
import multiprocessing as mp
import os
import pickle
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock

import pytest

from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.oras import Pusher, Registry, get_oras_client, oras
from conda_oci_mirror.package import Package
from conda_oci_mirror.repo import PackageRepo
from conda_oci_mirror.tasks import TaskRunner


def probe_worker(client):
    return client.get_tags(f"{client.hostname}/test"), client.prefix, os.getpid()


def test_mirrors_do_not_share_transport_or_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv("ORAS_USER", "alice")
    monkeypatch.setenv("ORAS_PASS", "first-test-password")
    original_prefix = oras.prefix
    first = Mirror(
        "test", [], registry="http://localhost:5000/first", cache_dir=tmp_path
    )
    monkeypatch.setenv("ORAS_USER", "bob")
    monkeypatch.setenv("ORAS_PASS", "second-test-password")
    second = Mirror(
        "test", [], registry="https://example.com/second", cache_dir=tmp_path
    )
    assert first.registry == "localhost:5000/first"
    assert first.client.prefix == "http"
    assert second.client.prefix == "https"
    assert first.client is not second.client
    assert first.client.session is not second.client.session
    assert (
        first.client.headers["Authorization"] != second.client.headers["Authorization"]
    )
    assert oras.prefix == original_prefix
    first.client.set_token_auth("test-token")
    assert second.client.token is None


def test_tasks_and_pushers_use_the_explicit_client(tmp_path, monkeypatch):
    mirror = Mirror(
        "test",
        [],
        subdirs=["noarch"],
        registry="http://localhost:5000/test",
        cache_dir=tmp_path,
    )
    monkeypatch.setattr(
        PackageRepo, "find_packages", lambda *args, **kwargs: [("demo-1-0.conda", {})]
    )
    queued = []

    def capture(runner):
        queued.extend(runner.tasks)
        return []

    monkeypatch.setattr(TaskRunner, "run_serial", capture)
    mirror.update(serial=True)
    assert queued[0].pkg.client is mirror.client
    assert queued[1].repo.client is mirror.client
    archive = tmp_path / "archive"
    archive.write_bytes(b"archive")
    push = Mock()
    monkeypatch.setattr(mirror.client, "push", push)
    pusher = Pusher(tmp_path, client=mirror.client)
    pusher.add_layer("archive", "test")
    cwd = os.getcwd()
    pusher.push("localhost:5000/test:1")
    assert os.getcwd() == cwd
    assert push.call_args.args[1][0]["path"] == str(archive)
    package = Package(
        "test", "noarch", "demo-1-0.conda", tmp_path, "http://localhost:5000/test"
    )
    assert package.client.prefix == "http"
    assert package.registry == "localhost:5000/test"


@pytest.mark.parametrize(
    "target",
    [
        "ftp://example.com",
        "https://user:secret@example.com",
        "https://example.com?query=1",
        "https://example.com#fragment",
    ],
)
def test_invalid_registry_urls_are_rejected(target):
    with pytest.raises(ValueError, match="Registry must"):
        get_oras_client(target)


def test_registry_override_has_its_own_transport(tmp_path, monkeypatch):
    repo = PackageRepo("test", "noarch", tmp_path, "http://example.com/namespace")
    calls = []

    def tags(client, uri, **kwargs):
        calls.append((client.prefix, uri))
        return []

    monkeypatch.setattr(Registry, "get_tags", tags)
    repo.get_existing_tags("demo")
    repo.get_existing_tags("demo", registry="https://example.com/namespace")
    assert calls == [
        ("http", "example.com/namespace/test/noarch/demo"),
        ("https", "example.com/namespace/test/noarch/demo"),
    ]
    assert repo.client.prefix == "http"


@pytest.mark.parametrize(
    "start_method", [m for m in ("spawn", "fork") if m in mp.get_all_start_methods()]
)
def test_worker_keeps_configuration_but_not_connection_pool(monkeypatch, start_method):
    monkeypatch.setenv("ORAS_USER", "test-user")
    monkeypatch.setenv("ORAS_PASS", "test-password")
    authorization = "Basic " + base64.b64encode(b"test-user:test-password").decode()
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append(self.headers.get("Authorization"))
            self.send_response(200 if received[-1] == authorization else 403)
            self.end_headers()
            self.wfile.write(b'{"tags": []}')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = get_oras_client(f"http://127.0.0.1:{server.server_port}/test")
        assert client.get_tags(f"{client.hostname}/test") == []
        restored = pickle.loads(pickle.dumps(client))
        assert restored._session is None
        assert restored.session is not client.session
        with mp.get_context(start_method).Pool(1) as pool:
            tags, prefix, pid = pool.apply_async(probe_worker, (client,)).get(
                timeout=20
            )
        assert tags == [] and prefix == "http" and pid != os.getpid()
        assert received == [authorization, authorization]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
