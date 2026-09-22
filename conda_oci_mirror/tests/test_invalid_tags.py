import json
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from conda_oci_mirror.cli import main
from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.oras import Registry
from conda_oci_mirror.package import Package, skip_invalid_tag
from conda_oci_mirror.repo import PackageRepo, RepoData
from conda_oci_mirror.tasks import TaskBase, TaskRunner

BAD_BUILD = "cuda129_py310* *_cpython_h93df00f_0"


@pytest.mark.parametrize(
    "tag,invalid",
    [
        ("0.4.1-" + BAD_BUILD, True),
        ("1-hello world", True),
        ("1-*", True),
        ("1-\n", True),
        ("1-ä", True),
        ("1-a/b", True),
        ("", True),
        (".latest", True),
        ("-latest", True),
        ("x" * 129, True),
        ("x" * 124 + "+", True),  # Validate length after existing escaping.
        ("x" * 128, False),
        ("_latest", False),
        ("0.4.1-cuda129_py310_h93df00f_0", False),
        ("1!2.0+local-py=0", False),
    ],
)
def test_tag_validation(tag, invalid):
    errors = []
    assert skip_invalid_tag(tag, "test/noarch/demo.conda", errors) == invalid
    assert len(errors) == int(invalid)
    assert all("\n" not in error for error in errors)


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize(
    "command,flags",
    [
        ("mirror", []),
        ("pull-cache", []),
        ("push-cache", []),
        ("push-cache", ["--push-all"]),
    ],
)
def test_skip_bad_builds_but_finish_valid_work(
    tmp_path, monkeypatch, caplog, command, flags, dry_run
):
    monkeypatch.setattr(
        "requests.sessions.Session.request",
        Mock(side_effect=AssertionError("Unexpected network request")),
    )
    monkeypatch.setattr(TaskBase, "wait", lambda *args: None)
    monkeypatch.setattr("conda_oci_mirror.tasks.time.sleep", lambda *args: None)
    monkeypatch.setattr(TaskRunner, "run", TaskRunner.run_serial)
    mirror = Mirror(
        "test",
        [],
        subdirs=["linux-64"],
        registry="example.com/test",
        cache_dir=tmp_path,
    )
    monkeypatch.setattr("conda_oci_mirror.cli.Mirror", lambda **kwargs: mirror)
    directory = tmp_path / "test" / "linux-64"
    directory.mkdir(parents=True)
    data = RepoData()
    bad_archives = []
    for name, build in [("groundingdino-py-cuda", BAD_BUILD), ("healthy", "0")]:
        for extension, section in [
            ("conda", "packages.conda"),
            ("tar.bz2", "packages"),
        ]:
            filename = f"{name}-0.4.1-{build}.{extension}"
            data.data[section][filename] = {
                "name": name,
                "version": "0.4.1",
                "build": build,
                "build_number": 0,
            }
            path = directory / filename
            path.write_bytes(b"untouched archive")
            if name != "healthy":
                bad_archives.append(path)
    # Keep the pull source separate so push-new sees all local archives as new.
    index = tmp_path / "upstream.json"
    original = json.dumps(data.data)
    index.write_text(original)
    monkeypatch.setattr(PackageRepo, "load_repodata", lambda *args: data)

    def tags(client, uri, **kwargs):
        assert uri.endswith(
            "/healthy"
        ), "Invalid builds must be skipped before registry queries"
        return []

    monkeypatch.setattr(Registry, "get_tags", tags)
    completed = []

    def ensure_file(package):
        assert package.package_name_bare == "healthy"
        package.file = str(directory / package.package)

    def upload(package, dry_run=False):
        assert package.package_name_bare == "healthy"
        completed.append("upload")
        return []

    def publish(repo, *args, **kwargs):
        assert completed == ["upload", "upload"]
        assert json.dumps(data.data) == original  # No silent repodata rewriting.
        completed.append("repodata")
        return []

    def pull(client, uri, *args):
        if uri.endswith("/repodata.json:latest"):
            return [str(index)]
        assert uri.endswith("/healthy:0.4.1-0")
        completed.append("pull")
        return [str(directory / "download")]

    monkeypatch.setattr(Package, "ensure_file", ensure_file)
    monkeypatch.setattr(Package, "upload", upload)
    monkeypatch.setattr(PackageRepo, "upload", publish)
    monkeypatch.setattr(Registry, "pull_by_media_type", pull)
    result = CliRunner().invoke(
        main, [command, "--quiet"] + flags + (["--dry-run"] if dry_run else [])
    )
    assert result.exit_code == 1, result.output
    assert "Skipped 2 invalid package tags" in result.output
    assert len(mirror.errors) == 2
    assert all(
        BAD_BUILD in error and "groundingdino-py-cuda" in error
        for error in mirror.errors
    )
    assert (
        len([record for record in caplog.records if record.levelname == "ERROR"]) == 2
    )
    assert all(path.read_bytes() == b"untouched archive" for path in bad_archives)
    if command == "pull-cache":
        assert completed == ([] if dry_run else ["pull", "pull"])
    else:
        assert completed == ["upload", "upload"] + (
            ["repodata"] if command == "mirror" and not dry_run else []
        )

    # Errors belong to one run and respect package selection.
    mirror.packages = ["healthy"]
    mirror.push_all(dry_run=True, serial=True)
    assert mirror.errors == []
