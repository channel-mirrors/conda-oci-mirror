import json
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from conda_oci_mirror.cli import main
from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.oras import Registry
from conda_oci_mirror.repo import PackageRepo, RepoData
from conda_oci_mirror.tasks import TaskRunner


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(
        "requests.sessions.Session.request",
        Mock(side_effect=AssertionError("Unexpected network request")),
    )


@pytest.mark.parametrize(
    "command,method",
    [
        ("mirror", "update"),
        ("pull-cache", "pull_latest"),
        ("push-cache", "push_new"),
    ],
)
def test_boolean_switches(monkeypatch, command, method):
    mirror = Mock(return_value=Mock(errors=[]))
    log = Mock()
    monkeypatch.setattr("conda_oci_mirror.cli.Mirror", mirror)
    monkeypatch.setattr("conda_oci_mirror.cli.setup_logger", log)
    result = CliRunner().invoke(main, [command, "--quiet", "--debug", "--dry-run"])
    assert result.exit_code == 0, result.output
    log.assert_called_once_with(quiet=True, debug=True)
    getattr(mirror.return_value, method).assert_called_once_with(True)


@pytest.mark.parametrize("flag", ["--push-all", "--all"])
def test_push_all_switch(monkeypatch, flag):
    mirror = Mock(return_value=Mock(errors=[]))
    monkeypatch.setattr("conda_oci_mirror.cli.Mirror", mirror)
    result = CliRunner().invoke(main, ["push-cache", flag])
    assert result.exit_code == 0, result.output
    mirror.return_value.push_all.assert_called_once_with(False)
    mirror.return_value.push_new.assert_not_called()


@pytest.mark.parametrize("flag", ["--upload-delay", "--timeout"])
def test_upload_delay_alias(monkeypatch, flag):
    mirror = Mock(return_value=Mock(errors=[]))
    monkeypatch.setattr("conda_oci_mirror.cli.Mirror", mirror)
    result = CliRunner().invoke(main, ["mirror", flag, "0", "--workers", "2"])
    assert result.exit_code == 0, result.output
    assert mirror.call_args.kwargs["timeout"] == 0
    assert mirror.call_args.kwargs["workers"] == 2


@pytest.mark.parametrize(
    "args",
    [
        ["--workers", "0"],
        ["--workers", "-1"],
        ["--upload-delay", "-1"],
        ["--timeout", "-1"],
    ],
)
def test_invalid_options_fail_before_work(monkeypatch, args):
    mirror = Mock()
    monkeypatch.setattr("conda_oci_mirror.cli.Mirror", mirror)
    result = CliRunner().invoke(main, ["mirror"] + args)
    assert result.exit_code == 2
    mirror.assert_not_called()


@pytest.mark.parametrize(
    "patterns,expected",
    [
        ([], {"alpha", "alpha-tools", "beta"}),
        (["alpha"], {"alpha"}),
        (["alp*"], {"alpha", "alpha-tools"}),
        (["alpha", "beta"], {"alpha", "beta"}),
        (["missing"], set()),
        ("alpha", {"alpha"}),
        ("all", {"alpha", "alpha-tools", "beta"}),
    ],
)
def test_selection_agrees_across_operations(tmp_path, monkeypatch, patterns, expected):
    mirror = Mirror(
        "test",
        patterns,
        subdirs=["noarch"],
        registry="example.com/test",
        cache_dir=tmp_path,
    )
    data = RepoData()
    directory = tmp_path / "test" / "noarch"
    directory.mkdir(parents=True)
    for name, extension in [
        ("alpha", "tar.bz2"),
        ("alpha", "conda"),
        ("alpha-tools", "conda"),
        ("beta", "conda"),
    ]:
        archive = f"{name}-1-0.{extension}"
        (directory / archive).write_bytes(b"archive")
        key = "packages" if extension == "tar.bz2" else "packages.conda"
        data.data[key][archive] = {
            "name": name,
            "version": "1",
            "build": "0",
            "build_number": 0,
        }
    index = directory / "repodata.json"
    index.write_text(json.dumps(data.data))
    original = {path: path.read_bytes() for path in directory.iterdir()}
    monkeypatch.setattr(PackageRepo, "load_repodata", lambda *args: data)
    monkeypatch.setattr(
        PackageRepo, "get_existing_packages", lambda *args, **kwargs: set()
    )
    monkeypatch.setattr(Registry, "pull_by_media_type", Mock(return_value=[str(index)]))
    queued = []

    def capture(runner):
        queued.extend(runner.tasks)
        return []

    def uploads():
        return [task for group in queued for task in [group] + group.following]

    monkeypatch.setattr(TaskRunner, "run_serial", capture)
    mirror.update(dry_run=True, serial=True)
    assert {task.pkg.package_info["name"] for task in uploads()} == expected
    assert len(uploads()) == len(expected) + ("alpha" in expected)
    queued.clear()
    mirror.pull_latest(serial=True)
    assert {task.uri.rsplit("/", 1)[-1].split(":")[0] for task in queued} == expected
    assert len(queued) == len(expected) + ("alpha" in expected)
    queued.clear()
    mirror.push_all(dry_run=True, serial=True)
    assert {task.pkg.package_name_bare for task in uploads()} == expected
    assert len(uploads()) == len(expected) + ("alpha" in expected)

    # No index: push_new must select the same archives, without changing them.
    index.unlink()
    queued.clear()
    mirror.push_new(dry_run=True, serial=True)
    assert {task.pkg.package_name_bare for task in uploads()} == expected
    assert {path: path.read_bytes() for path in directory.iterdir()} == {
        path: content for path, content in original.items() if path != index
    }
