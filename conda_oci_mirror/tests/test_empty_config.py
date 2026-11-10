import base64
import hashlib

import pytest

from conda_oci_mirror import defaults
from conda_oci_mirror.oras import (
    ARTIFACT_TYPE,
    EMPTY_CONFIG,
    get_oras_client,
    registry_name,
)


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


def test_push_uses_the_empty_descriptor(pushed, tmp_path):
    client, uri = pushed
    manifest = client.get_manifest(uri)
    assert manifest["config"] == EMPTY_CONFIG
    assert manifest["artifactType"] == ARTIFACT_TYPE
    # The blob is really there, for registries that do not honour `data`.
    blob = client.download_blob(uri, EMPTY_CONFIG["digest"], str(tmp_path / "config"))
    with open(blob, "rb") as config:
        assert config.read() == b"{}"
