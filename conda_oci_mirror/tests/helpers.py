import os
import sys

from conda_oci_mirror.oras import oras  # noqa

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)

import conda_oci_mirror.defaults as defaults  # noqa

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror  # noqa


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


def registry_host():
    return os.environ.get("registry_host") or "http://127.0.0.1"


def registry_port():
    return os.environ.get("registry_port") or 5000


def registry_url():
    return f"{registry_host()}:{registry_port()}"


def test_user():
    return "dinosaur"


def delete_tags(registry, channel, subdir, name):
    """
    Delete all tags for a repo
    """
    tags = f"{registry_url()}/{test_user()}/{channel}/{subdir}/{name}"
    try:
        tags = oras.get_tags(tags)
    except Exception:
        return

    for tag in tags:
        # get digest of manifest
        manifest_url = f"{registry_url()}/v2/{test_user()}/{channel}/{subdir}/{name}/manifests/{tag}"
        response = oras.do_request(
            manifest_url,
            "HEAD",
            headers={"Accept": "application/vnd.oci.image.manifest.v1+json"},
        )
        digest = response.headers.get("Docker-Content-Digest")
        delete_url = (
            f"{registry_url()}/v2/dinosaur/{channel}/{subdir}/{name}/manifests/{digest}"
        )
        response = oras.do_request(delete_url, "DELETE")

        if response.status_code != 202:
            print(response.text)
            raise RuntimeError("Failed to delete tag")


def get_mirror(cache_dir, subdir=None, package=None, channel=None):
    """
    Shared function to get a mirror for a particular subdir.
    """

    # Noarch is the only place redo exists
    channel = channel or "mirror-testing"

    # Interacting with a package repo means we interact with the
    # registry directly, the host + namespace.
    registry = f"{registry_url()}/{test_user()}"

    return Mirror(
        channel=channel,
        packages=[package] if package else None,
        registry=registry,
        subdirs=[subdir] if subdir else None,
        cache_dir=cache_dir,
    )
