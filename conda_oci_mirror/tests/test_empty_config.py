import base64
import hashlib
import os
from unittest.mock import Mock

import oras.defaults
import oras.oci
import pytest
import requests

from conda_oci_mirror import defaults
from conda_oci_mirror.oras import (
    ARTIFACT_TYPE,
    EMPTY_CONFIG,
    get_oras_client,
    registry_name,
)
from conda_oci_mirror.tasks import ConfigRewriteTask


def test_empty_descriptor_describes_its_content():
    assert base64.b64decode(EMPTY_CONFIG["data"]) == b"{}"
    assert EMPTY_CONFIG["size"] == 2
    assert EMPTY_CONFIG["digest"] == "sha256:" + hashlib.sha256(b"{}").hexdigest()


@pytest.fixture
def pushed(oci_registry, tmp_path):
    """A package tag pushed with the current code, and its client."""
    client = get_oras_client(oci_registry)
    archive = tmp_path / "demo-1.0-0.conda"
    archive.write_bytes(b"demo")
    uri = f"{registry_name(oci_registry)}/dinosaur/mirror-testing/noarch/demo:1.0-0"
    client.push(
        uri,
        [
            {
                "path": str(archive),
                "media_type": defaults.package_conda_media_type,
                "title": archive.name,
            }
        ],
    )
    return client, uri


def make_legacy(client, uri):
    """Rewrite a tag's manifest the way the mirror used to, with the zero-byte config."""
    manifest = client.get_manifest(uri)
    del manifest["artifactType"]
    container = client.get_container(uri)
    conf, config_file = oras.oci.ManifestConfig()
    client.upload_blob(config_file, container, conf)
    client._check_200_response(
        client.upload_manifest({**manifest, "config": conf}, container)
    )
    assert client.get_manifest(uri)["config"] == conf
    return manifest


def test_push_uses_the_empty_descriptor(pushed, tmp_path):
    client, uri = pushed
    manifest = client.get_manifest(uri)
    assert manifest["config"] == EMPTY_CONFIG
    assert manifest["artifactType"] == ARTIFACT_TYPE
    # The blob is really there, for registries that do not honour `data`.
    blob = client.download_blob(uri, EMPTY_CONFIG["digest"], str(tmp_path / "config"))
    with open(blob, "rb") as config:
        assert config.read() == b"{}"


def test_rewrite_config_replaces_the_legacy_config(pushed):
    client, uri = pushed
    legacy = make_legacy(client, uri)

    assert client.get_legacy_config_manifest(uri)["layers"] == legacy["layers"]
    assert client.rewrite_config(uri) is True
    manifest = client.get_manifest(uri)
    assert manifest["config"] == EMPTY_CONFIG
    assert manifest["artifactType"] == ARTIFACT_TYPE
    assert manifest["layers"] == legacy["layers"]

    assert client.get_legacy_config_manifest(uri) is None
    assert client.rewrite_config(uri) is False
    assert client.rewrite_config(uri.replace(":1.0-0", ":9.9-9")) is False


@pytest.fixture
def token_client(token_registry, monkeypatch):
    monkeypatch.setenv("ORAS_USER", "alice")
    monkeypatch.setenv("ORAS_PASS", "secret")
    client = get_oras_client(f"http://{token_registry['registry']}/test")
    return client, f"{token_registry['registry']}/test/shards"


LEGACY_CONFIG = {
    "mediaType": oras.defaults.unknown_config_media_type,
    "size": 0,
    "digest": oras.defaults.blank_hash,
}


def test_rewrite_does_not_write_with_the_token_of_the_read(
    token_registry, token_client
):
    client, repo = token_client
    token_registry["manifests"]["old"] = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": LEGACY_CONFIG,
        "layers": [],
    }
    # The registry answers a write that carries the read's pull-only token with 403.
    assert client.get_legacy_config_manifest(f"{repo}:old") is not None
    assert client.rewrite_config(f"{repo}:old") is True
    assert token_registry["manifests"]["old"]["config"] == EMPTY_CONFIG
    assert token_registry["manifests"]["old"]["artifactType"] == ARTIFACT_TYPE
    assert token_registry["blobs"][EMPTY_CONFIG["digest"]] == b"{}"


@pytest.mark.parametrize(
    "manifest",
    [
        # Not written by the mirror: a real config, and an index without one.
        {"config": {"mediaType": "a/b", "size": 7, "digest": "sha256:abc"}},
        {"manifests": []},
    ],
)
def test_only_the_zero_byte_config_is_rewritten(token_registry, token_client, manifest):
    client, repo = token_client
    token_registry["manifests"]["other"] = dict(manifest)
    assert client.rewrite_config(f"{repo}:other") is False
    assert token_registry["manifests"]["other"] == manifest


def test_a_failing_repository_keeps_the_other_results(
    token_registry, token_client, monkeypatch
):
    client, repo = token_client
    monkeypatch.setattr("conda_oci_mirror.decorators.time.sleep", lambda seconds: None)
    token_registry["manifests"]["old"] = {"config": LEGACY_CONFIG, "layers": []}

    def tags(uri, N=None):
        if uri.endswith("broken"):
            raise requests.ConnectionError("registry went away")
        return ["old", "missing"]

    monkeypatch.setattr(client, "get_tags", tags)
    results = [
        ConfigRewriteTask(uri, client, wait_time=0).run()
        for uri in (f"{repo}/broken", repo)
    ]
    assert results[0]["errors"] == ["ConnectionError: registry went away"]
    assert results[1] == {"uri": repo, "tags": 2, "rewritten": 1, "errors": []}


def test_permanent_errors_are_not_retried(token_registry, token_client, monkeypatch):
    client, repo = token_client
    sleeps = []
    monkeypatch.setattr("conda_oci_mirror.decorators.time.sleep", sleeps.append)
    calls = []

    def denied(uri, N=None):
        calls.append(uri)
        response = requests.Response()
        response.status_code = 403
        raise requests.HTTPError(response=response)

    monkeypatch.setattr(client, "get_tags", denied)
    result = ConfigRewriteTask(repo, client, wait_time=0).run()
    # A message-less error still counts and still says what it was.
    assert result["errors"] == ["HTTPError: "]
    assert len(calls) == 1
    assert sleeps == []


def test_exact_package_names_need_no_repodata(mirror_instance, monkeypatch):
    m = mirror_instance
    m.packages = ["redo", "gone-upstream"]
    monkeypatch.setattr(
        "conda_oci_mirror.repo.PackageRepo.load_repodata",
        Mock(side_effect=AssertionError("repodata downloaded")),
    )
    results = m.migrate_configs(dry_run=True, serial=True)
    assert [result["uri"].rsplit("/", 1)[1] for result in results] == [
        "gone-upstream",
        "redo",
    ]


def test_a_failing_tag_does_not_block_the_tags_after_it(
    token_registry, token_client, monkeypatch
):
    client, repo = token_client
    monkeypatch.setattr("conda_oci_mirror.decorators.time.sleep", lambda seconds: None)
    token_registry["manifests"]["old"] = {"config": LEGACY_CONFIG, "layers": []}
    rewrite = client.rewrite_config

    def flaky(uri):
        if ":bad" in uri:
            raise requests.ConnectionError("no")
        return rewrite(uri)

    monkeypatch.setattr(client, "rewrite_config", flaky)
    for tag in ("bad1", "bad2"):
        token_registry["manifests"][tag] = {"config": LEGACY_CONFIG, "layers": []}
    monkeypatch.setattr(client, "get_tags", lambda uri, N=None: ["bad1", "old", "bad2"])
    result = ConfigRewriteTask(repo, client, wait_time=0).run()
    assert result == {
        "uri": repo,
        "tags": 3,
        "rewritten": 1,
        "errors": ["bad1: ConnectionError: no", "bad2: ConnectionError: no"],
    }
    assert token_registry["manifests"]["old"]["config"] == EMPTY_CONFIG

    # When every tag fails it is the registry: stop instead of retrying them all.
    monkeypatch.setattr(
        client, "get_tags", lambda uri, N=None: [f"bad{n}" for n in range(50)]
    )
    for n in range(50):
        token_registry["manifests"][f"bad{n}"] = {"config": LEGACY_CONFIG, "layers": []}
    result = ConfigRewriteTask(repo, client, wait_time=0).run()
    assert result["tags"] == 5
    assert result["errors"][-1] == "stopped after 5 failed tags"


@pytest.fixture
def subdir():
    return "noarch"


def test_migrate_configs_rewrites_every_repository(mirror_instance):
    m = mirror_instance
    m.update(serial=True)
    client = m.client
    package_repo = f"{m.registry}/mirror-testing/noarch/redo"
    package_uri = f"{package_repo}:{client.get_tags(package_repo)[0]}"
    repodata_uri = f"{m.registry}/mirror-testing/noarch/repodata.json:latest"
    make_legacy(client, package_uri)
    make_legacy(client, repodata_uri)

    cached = os.path.join(m.cache_dir, "mirror-testing/noarch/repodata.json")
    written = os.stat(cached).st_mtime_ns

    dry_run = m.migrate_configs(dry_run=True, serial=True)
    assert sum(result["rewritten"] for result in dry_run) == 2
    assert client.get_manifest(package_uri)["config"]["size"] == 0

    results = m.migrate_configs(serial=True)
    assert sum(result["rewritten"] for result in results) == 2
    assert {result["uri"] for result in results} >= {
        package_uri.rsplit(":", 1)[0],
        repodata_uri.rsplit(":", 1)[0],
    }
    assert client.get_manifest(package_uri)["config"] == EMPTY_CONFIG
    assert client.get_manifest(repodata_uri)["config"] == EMPTY_CONFIG

    again = m.migrate_configs(serial=True)
    assert sum(result["rewritten"] for result in again) == 0
    assert not any(result["errors"] for result in results + again)
    # The repodata was only read to name the repositories.
    assert os.stat(cached).st_mtime_ns == written
