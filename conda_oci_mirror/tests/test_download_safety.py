import hashlib
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests
from click.testing import CliRunner

from conda_oci_mirror.cli import main
from conda_oci_mirror.oras import Registry
from conda_oci_mirror.package import _download_file_once
from conda_oci_mirror.tasks import DownloadTask, TaskBase


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(
        "requests.sessions.Session.request",
        Mock(side_effect=AssertionError("Unexpected network request")),
    )


@pytest.mark.parametrize("source", ["http", "oci", "oci-pull"])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("failure", [None, "checksum", "interrupted", "replace"])
def test_download_is_verified_and_atomic(
    tmp_path, monkeypatch, source, existing, failure
):
    destination = tmp_path / "nested" / "demo.conda"
    original = b"existing archive"
    content = b"new archive"
    if existing:
        destination.parent.mkdir()
        destination.write_bytes(original)
    expected = hashlib.sha256(
        content if failure != "checksum" else b"different"
    ).hexdigest()

    def assert_original():
        if existing:
            assert destination.read_bytes() == original
        else:
            assert not destination.exists()

    if source == "http":

        def chunks(**kwargs):
            assert_original()
            yield content[:3]
            assert_original()
            if failure == "interrupted":
                raise requests.ConnectionError("interrupted")
            yield content[3:]

        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_content.side_effect = chunks
        get = Mock(return_value=response)
        monkeypatch.setattr("conda_oci_mirror.package.requests.get", get)

        def download():
            return _download_file_once(
                "https://example.com/demo.conda", destination, {"sha256": expected}
            )

    else:
        monkeypatch.chdir(tmp_path)
        client = Registry()
        monkeypatch.setattr(
            client,
            "get_manifest",
            Mock(
                return_value={
                    "layers": [
                        {
                            "mediaType": "test",
                            "digest": "sha256:" + expected,
                            "annotations": {
                                "org.opencontainers.image.title": destination.name
                            },
                        }
                    ]
                }
            ),
        )

        def blob(self, container, digest, outfile):
            assert_original()
            Path(outfile).write_bytes(content)
            assert_original()
            if failure == "interrupted":
                raise requests.ConnectionError("interrupted")
            return outfile

        monkeypatch.setattr("oras.provider.Registry.download_blob", blob)
        monkeypatch.setattr(client, "load_configs", Mock())

        def download():
            if source == "oci-pull":
                return client.pull(
                    target="example.com/test/demo:1", outdir=str(destination.parent)
                )[0]
            return client.pull_by_media_type(
                "example.com/test/demo:1", "nested", "test"
            )[0]

    if failure == "replace":
        monkeypatch.setattr(
            "os.replace", Mock(side_effect=PermissionError("replace failed"))
        )
    if failure:
        errors = {
            "checksum": (ValueError, RuntimeError),
            "interrupted": requests.ConnectionError,
            "replace": PermissionError,
        }
        with pytest.raises(errors[failure]):
            download()
        assert_original()
    else:
        assert Path(download()) == destination
        assert destination.read_bytes() == content

    # No partial archives or staging directories survive success or failure.
    assert list(destination.parent.iterdir()) == (
        [destination] if existing or not failure else []
    )
    if source == "http":
        assert get.call_args.kwargs["timeout"] == 60


def test_matching_cached_blob_is_not_downloaded(tmp_path, monkeypatch):
    destination = tmp_path / "demo.conda"
    destination.write_bytes(b"cached")
    client = Registry()
    monkeypatch.setattr(
        client,
        "get_manifest",
        Mock(
            return_value={
                "layers": [
                    {
                        "mediaType": "test",
                        "digest": "sha256:" + hashlib.sha256(b"cached").hexdigest(),
                        "annotations": {
                            "org.opencontainers.image.title": destination.name
                        },
                    }
                ]
            }
        ),
    )
    download = Mock()
    monkeypatch.setattr(client, "download_blob", download)
    assert client.pull_by_media_type("example.com/test/demo:1", str(tmp_path)) == [
        str(destination)
    ]
    download.assert_not_called()


@pytest.mark.parametrize("artifact", ["../escape", "/escape"])
def test_unsafe_blob_path_is_rejected(tmp_path, monkeypatch, artifact):
    client = Registry()
    monkeypatch.setattr(
        client,
        "get_manifest",
        Mock(
            return_value={
                "layers": [
                    {
                        "mediaType": "test",
                        "digest": "sha256:" + "0" * 64,
                        "annotations": {"org.opencontainers.image.title": artifact},
                    }
                ]
            }
        ),
    )
    download = Mock()
    monkeypatch.setattr(client, "download_blob", download)
    with pytest.raises(Exception, match="not in"):
        client.pull_by_media_type("example.com/test/demo:1", str(tmp_path))
    download.assert_not_called()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("error", [None, requests.ConnectionError("unreachable")])
def test_download_task_reports_missing_or_failed_blobs(tmp_path, monkeypatch, error):
    monkeypatch.setattr(TaskBase, "wait", lambda *args: None)
    monkeypatch.setattr(
        Registry, "pull_by_media_type", Mock(return_value=[], side_effect=error)
    )
    with pytest.raises(requests.ConnectionError if error else ValueError):
        DownloadTask("example.com/test/demo:1", str(tmp_path), "test").run()


@pytest.mark.parametrize("error", [None, requests.HTTPError("unauthorized")])
def test_pull_cli_fails_when_repodata_is_unavailable(tmp_path, monkeypatch, error):
    monkeypatch.setattr(
        Registry, "pull_by_media_type", Mock(return_value=[], side_effect=error)
    )
    result = CliRunner().invoke(
        main,
        [
            "pull-cache",
            "--channel",
            "test",
            "--subdir",
            "noarch",
            "--registry",
            "example.com/test",
            "--cache-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, requests.HTTPError if error else ValueError)
