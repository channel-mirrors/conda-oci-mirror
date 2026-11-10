from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest
from xprocess import ProcessStarter

from conda_oci_mirror import defaults
from conda_oci_mirror.mirror import Mirror

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)


def check_media_type(layer):
    """
    Ensure the layer media type matches what we expect
    """
    if layer["path"].endswith("repodata.json"):
        assert layer["media_type"] == defaults.repodata_media_type_v1
    elif layer["path"].endswith("repodata.json.zst"):
        assert layer["media_type"] == defaults.repodata_media_type_v1_zst
    elif layer["path"].endswith("repodata_shards.msgpack.zst"):
        assert layer["media_type"] == defaults.repodata_shards_media_type_v1
    elif "/shards/" in str(layer["path"]):
        assert layer["media_type"] == defaults.repodata_shard_media_type_v1
    elif layer["path"].endswith("conda"):
        assert layer["media_type"] == defaults.package_conda_media_type
    elif layer["path"].endswith("bz2"):
        assert layer["media_type"] == defaults.package_tarbz2_media_type
    elif layer["path"].endswith("info.tar.gz"):
        assert layer["media_type"] == defaults.info_archive_media_type
    elif layer["path"].endswith("index.json"):
        assert layer["media_type"] == defaults.info_index_media_type
    else:
        raise ValueError(f"Unexpected layer content type {layer}")


def registry_host():
    return os.environ.get("registry_host") or "http://127.0.0.1"


def registry_port():
    return os.environ.get("registry_port") or 5000


def registry_url():
    return f"{registry_host()}:{registry_port()}"


def test_user():
    return "dinosaur"


@pytest.fixture
def oci_registry(xprocess):
    external = os.environ.get("CONDA_OCI_TEST_REGISTRY")
    if external:
        # Isolate tests even when they share one externally managed registry.
        yield f"{external.rstrip('/')}/test-{uuid.uuid4().hex}"
        return

    class Starter(ProcessStarter):
        # startup pattern
        pattern = r".*listening on \[::\]:5000.*"

        # command to start process
        args: ClassVar[list[str]] = [
            "docker",
            "run",
            "--rm",
            "-p",
            "5010:5000",
            "-e",
            "REGISTRY_STORAGE_DELETE_ENABLED=true",
            "registry:2",
        ]

    # ensure process is running and return its logfile
    xprocess.ensure("oci_registry", Starter)

    conn = "http://localhost:5010"
    yield conn

    # clean up whole process tree afterwards
    xprocess.getinfo("oci_registry").terminate()


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
            # A read is challenged for a pull-only token, which cannot write.
            read = self.command == "GET"
            sent = self.headers.get("Authorization")
            if sent == f"Bearer {state['token']}":
                return True
            if sent == f"Bearer {state['token']}-pull":
                if read:
                    return True
                self.reply(403)
                return False
            host = f"127.0.0.1:{self.server.server_port}"
            scope = "pull" if read else "push,pull"
            self.reply(
                401,
                headers={
                    "Www-Authenticate": f'Bearer realm="http://{host}/token",'
                    f'service="{host}",scope="repository:test/shards:{scope}"'
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
                suffix = "" if "push" in self.path else "-pull"
                body = json.dumps({"token": state["token"] + suffix}).encode()
                return self.reply(200 if ok else 403, body if ok else b"")
            if not self.authorized():
                return
            manifest = state["manifests"].get(self.path.rsplit("/", 1)[1])
            if "/manifests/" in self.path and manifest is not None:
                return self.reply(200, json.dumps(manifest).encode())
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


@pytest.fixture
def subdir():
    return None


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path


@pytest.fixture
def mirror_instance(subdir, oci_registry, cache_dir):
    """
    Shared function to get a mirror for a particular subdir.
    """
    package = None
    channel = None
    print("Creating mirror with subdir", subdir, oci_registry)
    # Noarch is the only place redo exists
    channel = channel or "mirror-testing"

    # Interacting with a package repo means we interact with the
    # registry directly, the host + namespace.
    registry = f"{oci_registry}/{test_user()}"

    return Mirror(
        channel=channel,
        packages=[package] if package else None,
        registry=registry,
        subdirs=[subdir] if subdir else None,
        cache_dir=cache_dir,
    )
