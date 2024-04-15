import os
import sys

import pytest
from xprocess import ProcessStarter

import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)


def check_media_type(layer):
    """
    Ensure the layer media type matches what we expect
    """
    if layer["path"].endswith("repodata.json"):
        assert layer["media_type"] == defaults.repodata_media_type_v1
    elif layer["path"].endswith("repodata.json.zst"):
        assert layer["media_type"] == defaults.repodata_media_type_v1_zst
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


@pytest.fixture
def oci_registry(xprocess):
    class Starter(ProcessStarter):
        # startup pattern
        pattern = r".*listening on \[::\]:5000.*"

        # command to start process
        args = [
            "docker",
            "run",
            "--rm",
            "-p",
            "5010:5000",
            "-e",
            "REGISTRY_STORAGE_DELETE_ENABLED=true",
            "registry:2",
        ]

    # ensure process is running and return its logfile
    xprocess.ensure("oci_registry", Starter)

    conn = "http://localhost:5010"
    yield conn

    # clean up whole process tree afterwards
    xprocess.getinfo("oci_registry").terminate()


@pytest.fixture
def subdir():
    return None


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path


@pytest.fixture
def mirror_instance(subdir, oci_registry, cache_dir):
    """
    Shared function to get a mirror for a particular subdir.
    """
    package = None
    channel = None
    print("Creating mirror with subdir", subdir, oci_registry)
    # Noarch is the only place redo exists
    channel = channel or "mirror-testing"

    # Interacting with a package repo means we interact with the
    # registry directly, the host + namespace.
    registry = f"{oci_registry}/{test_user()}"

    return Mirror(
        channel=channel,
        packages=[package] if package else None,
        registry=registry,
        subdirs=[subdir] if subdir else None,
        cache_dir=cache_dir,
    )
