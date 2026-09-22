import json
import tarfile
from pathlib import Path
from unittest.mock import Mock

import pytest
from rattler import Version

from conda_oci_mirror import defaults
from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.oras import Pusher, oras
from conda_oci_mirror.package import Package, package_reference
from conda_oci_mirror.repo import PackageRepo, RepoData
from conda_oci_mirror.tasks import TaskRunner


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(
        "requests.sessions.Session.request",
        Mock(side_effect=AssertionError("Network forbidden")),
    )


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("zlib:1.2-0", "zlib:1.2-0"),
        ("_lib:1!2.0+local-build=1", "zzz_lib:1__e__2.0__p__local-build__eq__1"),
        (
            "zzz_lib:1__e__2.0__p__local-build__eq__1",
            "zzz_lib:1__e__2.0__p__local-build__eq__1",
        ),
        ("_lib", "zzz_lib"),
    ],
)
def test_reference_spelling(raw, expected):
    assert package_reference(raw) == expected


def test_upload_and_read_use_the_same_reference(tmp_path, monkeypatch):
    archive = tmp_path / "_lib-1!2.0+local-build=1.conda"
    archive.write_bytes(b"archive")
    package = Package(
        "test",
        "noarch",
        archive.name,
        tmp_path,
        "example.com/test",
        existing_file=str(archive),
    )

    def metadata(self, stage):
        root = Path(stage) / self.package_name
        (root / "info").mkdir(parents=True)
        (root / "info" / "index.json").write_text('{"subdir": "noarch"}')
        with tarfile.open(root / "info.tar.gz", "w:gz"):
            pass

    monkeypatch.setattr(Package, "prepare_metadata", metadata)
    monkeypatch.setattr(Pusher, "push", lambda self, uri: uri)
    expected = "example.com/test/test/noarch/zzz_lib:1__e__2.0__p__local-build__eq__1"
    assert package.upload() == [expected]
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test")
    index = tmp_path / "index.json"
    index.write_text("{}")
    info = tmp_path / "info.tar.gz"
    with tarfile.open(info, "w:gz"):
        pass
    pull = Mock(side_effect=[[str(index)], [str(info)], [str(archive)]])
    monkeypatch.setattr(oras, "pull_by_media_type", pull)
    raw = "_lib:1!2.0+local-build=1"
    assert repo.get_index_json(raw) == {}
    with repo.get_info(raw):
        pass
    assert repo.get_package(raw) == str(archive)
    assert all(call.args[0] == expected for call in pull.call_args_list)
    assert pull.call_args_list[-1].args[2] == defaults.package_conda_media_type


def records():
    data = RepoData()
    for version, build_number, ext in [
        ("1.0dev1", 99, "tar.bz2"),
        ("1.0", 0, "tar.bz2"),
        ("1.0_5", 0, "tar.bz2"),
        ("1.0_5", 1, "tar.bz2"),
        ("1!0.1+local", 0, "conda"),
        ("1!0.1+local", 1, "conda"),
    ]:
        filename = f"_lib-{version}-{build_number}.{ext}"
        key = "packages" if ext == "tar.bz2" else "packages.conda"
        data.data[key][filename] = {
            "name": "_lib",
            "version": version,
            "build_number": build_number,
            "build": str(build_number),
        }
    return data


def test_latest_uses_conda_order_and_keeps_formats_separate(monkeypatch):
    data = records()
    parse = Mock(side_effect=Version)
    monkeypatch.setattr("conda_oci_mirror.repo.Version", parse)
    latest = {name for name, _ in data.latest_packages()}
    assert latest == {"_lib-1.0_5-1.tar.bz2", "_lib-1!0.1+local-1.conda"}
    assert parse.call_count == 4  # Parse each distinct version only once.
    assert data.get_latest_tag("_lib") == "1!0.1+local-1"
    assert data.get_latest_tag("_lib", "tar.bz2") == "1.0_5-1"
    assert data.get_latest_tag("missing") is None


def test_pull_schedules_only_latest_available_format(tmp_path, monkeypatch):
    index = tmp_path / "repodata.json"
    index.write_text(json.dumps(records().data))
    monkeypatch.setattr(oras, "pull_by_media_type", Mock(return_value=[str(index)]))
    queued = []

    def capture(runner):
        queued.extend(runner.tasks)
        return []

    monkeypatch.setattr(TaskRunner, "run_serial", capture)
    mirror = Mirror(
        "test",
        ["_lib"],
        subdirs=["noarch"],
        registry="example.com/test",
        cache_dir=tmp_path,
    )
    mirror.pull_latest(serial=True)
    assert {(task.uri.rsplit("/", 1)[-1], task.media_type) for task in queued} == {
        ("zzz_lib:1.0_5-1", defaults.package_tarbz2_media_type),
        ("zzz_lib:1__e__0.1__p__local-1", defaults.package_conda_media_type),
    }
    queued.clear()
    mirror.pull_latest(serial=True, dry_run=True)
    assert not queued
