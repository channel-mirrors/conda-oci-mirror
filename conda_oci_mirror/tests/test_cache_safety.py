import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.package import Package
from conda_oci_mirror.tasks import PackageUploadTask, TaskBase


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("Unexpected network request")

    monkeypatch.setattr("requests.sessions.Session.request", no_network)
    monkeypatch.setattr(
        "subprocess.check_output",
        Mock(side_effect=AssertionError("Unexpected reindex")),
    )
    monkeypatch.setattr(TaskBase, "wait", lambda *args: None)
    monkeypatch.setattr("conda_oci_mirror.tasks.time.sleep", lambda seconds: None)


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("cleanup", [False, True])
def test_upload_task_cleanup_is_opt_in(tmp_path, monkeypatch, dry_run, cleanup):
    archive = tmp_path / "demo-1.0-0.conda"
    archive.write_bytes(b"local archive")
    package = Package(
        "test",
        "noarch",
        archive.name,
        tmp_path,
        "example.com/test",
        existing_file=str(archive),
    )
    upload = Mock(return_value=[])
    monkeypatch.setattr(package, "upload", upload)

    PackageUploadTask(package, dry_run=dry_run, cleanup=cleanup).run()

    upload.assert_called_once_with(dry_run)
    assert archive.exists() is not cleanup


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("push_all", [False, True])
@pytest.mark.parametrize("with_index", [False, True])
def test_cache_push_preserves_every_subdir(
    tmp_path, monkeypatch, dry_run, push_all, with_index
):
    mirror = Mirror(
        "test",
        [],
        subdirs=["linux-64", "osx-64"],
        registry="example.com/test",
        cache_dir=tmp_path,
    )
    for subdir, cache_dir in mirror.iter_subdirs():
        directory = Path(cache_dir)
        directory.mkdir(parents=True)
        (directory / "known-1.0-0.conda").write_bytes(b"known archive")
        (directory / "new-1.0-0.tar.bz2").write_bytes(b"new archive")
        if with_index:
            (directory / "repodata.json").write_text(
                json.dumps({"packages.conda": {"known-1.0-0.conda": {"name": "known"}}})
            )

    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    uploaded = []

    def upload(package, dry_run=False):
        uploaded.append((package.subdir, Path(package.file).name, dry_run))
        return []

    monkeypatch.setattr(Package, "upload", upload)
    mirror.push(dry_run=dry_run, push_all=push_all, serial=True)

    expected_names = {"new-1.0-0.tar.bz2"}
    if push_all or not with_index:
        expected_names.add("known-1.0-0.conda")
    assert set(uploaded) == {
        (subdir, name, dry_run) for subdir in mirror.subdirs for name in expected_names
    }
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


def test_failed_cache_push_preserves_files(tmp_path, monkeypatch):
    mirror = Mirror(
        "test",
        [],
        subdirs=["noarch"],
        registry="example.com/test",
        cache_dir=tmp_path,
    )
    directory = tmp_path / "test" / "noarch"
    directory.mkdir(parents=True)
    (directory / "demo-1.0-0.conda").write_bytes(b"local archive")
    (directory / "repodata.json").write_text('{"packages": {}}')
    before = {p: p.read_bytes() for p in directory.iterdir()}
    monkeypatch.setattr(
        Package, "upload", Mock(side_effect=RuntimeError("upload failed"))
    )

    with pytest.raises(RuntimeError, match="upload failed"):
        mirror.push_all(serial=True)

    assert {p: p.read_bytes() for p in directory.iterdir()} == before
