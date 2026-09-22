import copy
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from conda_oci_mirror import defaults
from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.oras import Pusher, Registry
from conda_oci_mirror.package import Package
from conda_oci_mirror.repo import PackageRepo, RepoData
from conda_oci_mirror.tasks import PackageUploadTask, TaskBase, TaskRunner


def response(status):
    result = requests.Response()
    result.status_code = status
    result._content = b"{}"
    return result


@pytest.fixture
def registry(tmp_path, monkeypatch):
    client = Registry()
    manifests = {}

    def publish(manifest, container):
        manifests[container.uri] = copy.deepcopy(manifest)
        return response(201)

    def manifest(container):
        uri = client.get_container(container).uri
        if uri not in manifests:
            response(404).raise_for_status()
        return copy.deepcopy(manifests[uri])

    monkeypatch.setattr(client, "get_manifest", Mock(side_effect=manifest))
    monkeypatch.setattr(client, "upload_manifest", Mock(side_effect=publish))
    monkeypatch.setattr(client, "upload_blob", Mock(return_value=response(201)))
    monkeypatch.setattr(
        "requests.sessions.Session.request",
        Mock(side_effect=AssertionError("Network forbidden")),
    )
    monkeypatch.setattr(TaskBase, "wait", lambda *args: None)
    monkeypatch.setattr("conda_oci_mirror.tasks.time.sleep", lambda seconds: None)

    def metadata(self, stage):
        directory = Path(stage) / self.package_name
        (directory / "info").mkdir(parents=True)
        (directory / "info" / "index.json").write_text('{"subdir":"noarch"}')
        (directory / "info.tar.gz").write_bytes(b"metadata")

    monkeypatch.setattr(Package, "prepare_metadata", metadata)
    return client, manifests


@pytest.mark.parametrize("extensions", [("conda", "tar.bz2"), ("tar.bz2", "conda")])
def test_both_formats_survive_uploads_and_reuploads(tmp_path, registry, extensions):
    client, manifests = registry
    packages = []
    runner = TaskRunner()
    for ext in extensions:
        archive = tmp_path / f"demo-1-0.{ext}"
        archive.write_bytes(ext.encode())
        package = Package(
            "test",
            "noarch",
            archive.name,
            tmp_path,
            "example.com/test",
            existing_file=str(archive),
            client=client,
            new_tag=True,
        )
        packages.append(package)
        runner.add_task(PackageUploadTask(package))
    assert len(runner.tasks) == 1
    assert len(runner.tasks[0].following) == 1
    assert packages[0].new_tag and not packages[1].new_tag
    assert len(runner.run_serial()) == 2
    # A new single-format upload uses no extra manifest request; only the sibling reads it.
    assert client.get_manifest.call_count == 1
    uri = "example.com/test/test/noarch/demo:1-0"
    manifest = manifests[uri]
    archive_types = {
        defaults.package_conda_media_type,
        defaults.package_tarbz2_media_type,
    }
    layers = {
        layer["mediaType"]: layer
        for layer in manifest["layers"]
        if layer["mediaType"] in archive_types
    }
    assert set(layers) == archive_types
    assert {
        layer["annotations"]["org.opencontainers.image.title"]
        for layer in layers.values()
    } == {"demo-1-0.conda", "demo-1-0.tar.bz2"}
    # A separate push defaults to preservation, even when it only has one format locally.
    assert not packages[0].new_tag
    packages[0].upload()
    assert {
        layer["mediaType"]: layer
        for layer in manifests[uri]["layers"]
        if layer["mediaType"] in archive_types
    } == layers


def test_retry_does_not_keep_assuming_tag_absence(tmp_path, registry, monkeypatch):
    client, _ = registry
    archive = tmp_path / "demo-1-0.conda"
    archive.write_bytes(b"archive")
    package = Package(
        "test",
        "noarch",
        archive.name,
        tmp_path,
        "example.com/test",
        existing_file=str(archive),
        client=client,
        new_tag=True,
    )
    attempts = []

    def push(pusher, uri):
        attempts.append(pusher.preserve_existing)
        if len(attempts) == 1:
            raise requests.ConnectionError("Response lost after a possible commit")
        return {"uri": uri}

    monkeypatch.setattr(Pusher, "push", push)
    assert len(package.upload()) == 1
    assert attempts == [False, True]


def test_collision_scan_repairs_only_the_missing_format(
    tmp_path, monkeypatch, registry
):
    client, manifests = registry
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test", client=client)
    data = RepoData()
    for ext, section in [("conda", "packages.conda"), ("tar.bz2", "packages")]:
        data.data[section][f"demo-1-0.{ext}"] = {
            "name": "demo",
            "version": "1",
            "build": "0",
            "build_number": 0,
        }
    monkeypatch.setattr(repo, "load_repodata", lambda *args: data)
    monkeypatch.setattr(client, "get_tags", Mock(return_value=["1-0"]))
    uri = "example.com/test/test/noarch/demo:1-0"
    manifests[uri] = {"layers": [{"mediaType": defaults.package_conda_media_type}]}
    assert [filename for filename, _ in repo.find_packages()] == ["demo-1-0.tar.bz2"]
    assert not repo.new_archives  # The tag exists, so its sibling must be preserved.
    assert client.get_manifest.call_count == 1
    assert repo.get_existing_packages("demo", package_ext="tar.bz2") == set()
    assert repo.get_existing_packages("demo") == {"demo-1-0.conda"}
    manifests[uri]["layers"].append({"mediaType": defaults.package_tarbz2_media_type})
    assert list(repo.find_packages()) == []  # A new scan refreshes descriptors.


def test_single_format_planning_keeps_the_tag_only_fast_path(
    tmp_path, monkeypatch, registry
):
    client, _ = registry
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test", client=client)
    data = RepoData()
    data.data["packages.conda"]["demo-1-0.conda"] = {"name": "demo"}
    monkeypatch.setattr(repo, "load_repodata", lambda *args: data)
    monkeypatch.setattr(client, "get_tags", Mock(return_value=["1-0"]))
    assert list(repo.find_packages()) == []
    client.get_manifest.assert_not_called()
    monkeypatch.setattr(client, "get_tags", Mock(return_value=[]))
    assert len(list(repo.find_packages())) == 1
    assert repo.new_archives == {"demo-1-0.conda"}
    client.get_manifest.assert_not_called()


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_manifest_read_errors_do_not_overwrite_a_tag(
    tmp_path, registry, monkeypatch, status
):
    client, manifests = registry
    error = requests.HTTPError(response=response(status))
    monkeypatch.setattr(client, "get_manifest", Mock(side_effect=error))
    archive = tmp_path / "demo.conda"
    archive.write_bytes(b"archive")
    with pytest.raises(requests.HTTPError):
        client.push(
            "example.com/test/demo:1",
            [{"path": str(archive), "media_type": defaults.package_conda_media_type}],
        )
    assert not manifests
    client.upload_manifest.assert_not_called()


def test_mirror_marks_new_tags_without_extra_requests(tmp_path, registry, monkeypatch):
    client, _ = registry
    client.has_auth = False
    monkeypatch.setattr(
        "conda_oci_mirror.mirror.get_oras_client", Mock(return_value=client)
    )
    data = RepoData()
    data.data["packages.conda"]["demo-1-0.conda"] = {"name": "demo"}
    monkeypatch.setattr(PackageRepo, "load_repodata", lambda *args: data)
    monkeypatch.setattr(client, "get_tags", Mock(return_value=[]))
    queued = []

    def capture(runner):
        queued.extend(runner.tasks)
        return []

    monkeypatch.setattr(TaskRunner, "run_serial", capture)
    mirror = Mirror(
        "test", [], subdirs=["noarch"], registry="example.com/test", cache_dir=tmp_path
    )
    mirror.update(dry_run=True, serial=True)
    assert len(queued) == 1 and queued[0].pkg.new_tag
    client.get_manifest.assert_not_called()
