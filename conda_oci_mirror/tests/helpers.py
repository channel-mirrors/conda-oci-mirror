import os
import sys

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)

import conda_oci_mirror.defaults as defaults  # noqa

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror  # noqa
from conda_oci_mirror.oras import oras  # noqa


def check_media_type(layer):
    """
    Ensure the layer media type matches what we expect
    """
    if layer["path"].endswith("repodata.json"):
        assert layer["media_type"] == defaults.repodata_media_type_v1
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


def get_mirror(cache_dir, subdir=None, package=None, channel=None):
    """
    Shared function to get a mirror for a particular subdir.
    """
    registry_host = os.environ.get("registry_host") or "http://127.0.0.1"
    registry_port = os.environ.get("registry_port") or 5000
    host = f"{registry_host}:{registry_port}"

    # Noarch is the only place redo exists
    channel = channel or "mirror-testing"
    user = "dinosaur"

    # Interacting with a package repo means we interact with the
    # registry directly, the host + namespace.
    registry = f"{host}/{user}"

    return Mirror(
        channel=channel,
        packages=[package] if package else None,
        registry=registry,
        subdirs=[subdir] if subdir else None,
        cache_dir=cache_dir,
    )


def get_tags(uri):
    """
    Helper function to get tags
    """
    result = oras.get_tags(uri)
    assert result.status_code == 200
    result = result.json()
    assert "tags" in result
    return result
