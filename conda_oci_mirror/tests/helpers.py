import os
import sys

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror  # noqa
from conda_oci_mirror.oras import oras  # noqa


def get_mirror(subdir, cache_dir, package=None, channel=None):
    """
    Shared function to get a mirror for a particular subdir.
    """
    registry_host = os.environ.get("registry_host") or "http://127.0.0.1"
    registry_port = os.environ.get("registry_port") or 5000
    host = f"{registry_host}:{registry_port}"

    # Noarch is the only place redo exists
    channel = channel or "conda-forge"
    package = package or "redo"
    user = "dinosaur"

    # Interacting with a package repo means we interact with the
    # registry directly, the host + namespace.
    registry = f"{host}/{user}"

    return Mirror(
        channel=channel,
        packages=[package],
        registry=registry,
        subdirs=[subdir],
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
