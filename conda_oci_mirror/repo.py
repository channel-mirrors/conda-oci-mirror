# Packages and functions for them

import datetime
import fnmatch
import os
import tarfile

import requests

import conda_oci_mirror.decorators as decorators
import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.util as util
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import Pusher, oras

# This is shared between PackageRepo instances
existing_tags_cache = {}


class PackageRepo:
    """
    A package repository manages a conda package repository.

    Note that a PackageRepo can be used as the previous "SubdirAccessor"
    """

    def __init__(self, channel, subdir, cache_dir, registry=None):
        self.channel = channel
        self.subdir = subdir
        self.cache_dir = cache_dir or defaults.CACHE_DIR
        self.timestamp = None

        # Can be over-ridden by upload/tags/packages functions if desired
        self.registry = registry

        # Should the registry requests use http or https?
        global oras
        insecure = True if self.registry.startswith("http://") else False
        if insecure:
            oras.set_insecure()

    @property
    def repodata(self):
        return os.path.join(self.cache_dir, "repodata.json")

    @property
    def name(self):
        return os.path.join(self.channel, self.subdir)

    def exists(self):
        return os.path.exists(self.repodata)

    @decorators.require_registry
    def get_index_json(self, package):
        """
        Get the index.json for a particular package
        """
        container = f"{self.registry}/{self.channel}/{self.subdir}/{package}"

        # We pull to the higher up cache directory, which should extract to cache
        # E.g., '/tmp/pytest-of-vanessa/pytest-19/test_package_repo_linux_64_0/cache
        # and we extract '<ditto>/cache/zlib-1.2.11-0/info/index.json
        res = oras.pull_by_media_type(
            container, self.cache_dir, defaults.info_index_media_type
        )
        if not res:
            raise ValueError(
                f"Cannot pull {container} {defaults.info_index_media_type}, does not exist."
            )
        return util.read_json(res[0])

    @decorators.require_registry
    def get_info(self, package):
        """
        Get the package info, returns an opened tarfile.

        We can change this to be something else (e.g., member retrieval) if desired.
        """
        container = f"{self.registry}/{self.channel}/{self.subdir}/{package}"
        res = oras.pull_by_media_type(
            container, self.cache_dir, defaults.info_archive_media_type
        )
        if not res:
            raise ValueError(
                f"Cannot pull {container} {defaults.info_archive_media_type}, does not exist."
            )
        return tarfile.open(res[0], "r:gz")

    @decorators.require_registry
    def get_package(self, package):
        """
        Get the pull package .tar.bz2 file
        """
        container = f"{self.registry}/{self.channel}/{self.subdir}/{package}"
        res = oras.pull_by_media_type(
            container, self.cache_dir, defaults.package_tarbz2_media_type
        )
        if not res:
            raise ValueError(
                f"Cannot pull {container} {defaults.package_tarbz2_media_type}, does not exist."
            )
        return res[0]

    def ensure_timestamp(self):
        """
        Ensure we have a timestamp when it was downloaded.
        """
        if self.exists():
            self.timestamp = datetime.datetime.fromtimestamp(
                os.stat(self.repodata).st_ctime
            )
            return
        self.timestamp = datetime.datetime.now()

    def get_repodata(self):
        """
        Get respository metadata
        """
        # TODO we should have a check here for timestamp, and re-retrieve if older than X
        util.mkdir_p(os.path.dirname(self.repodata))
        r = requests.get(
            f"https://conda.anaconda.org/{self.channel}/{self.subdir}/repodata.json",
            allow_redirects=True,
        )
        util.write_file(r.text, self.repodata)
        self.ensure_timestamp()
        return self.repodata

    def upload(self, root, registry=None):
        """
        Push the repodata.json to a named registry from root context.
        """
        registry = registry or self.registry
        repodata = self.get_repodata()
        pushes = []

        # title is used for archive name (path extracted to) so relative to root
        title = os.path.relpath(repodata, root)

        # Push should be relative to cache context
        uri = f"{registry}/{self.channel}/{self.subdir}/repodata.json"

        # Don't be pushy now, or actually, do :)
        pusher = Pusher(root, self.timestamp)
        pusher.add_layer(repodata, defaults.repodata_media_type_v1, title)

        # Push for a tag for the date, and latest
        for tag in pusher.created_at, "latest":
            logger.info(f"  pushing tag {tag}")
            pushes.append(pusher.push(f"{uri}:{tag}"))

        # Return pushes
        return pushes

    def load_repodata(self):
        """
        Load repository data (json)
        """
        if not self.exists():
            self.get_repodata()
        return util.read_json(self.repodata)

    def find_packages(self, names=None, skips=None, registry=None):
        """
        Given loaded repository data, find packages of interest
        """
        registry = registry or self.registry
        skips = skips or []
        data = self.load_repodata()

        # Look through package info
        for pkg, info in data.get("packages", {}).items():

            # Case 1: we are given packages to filter to
            if names:
                if not any(fnmatch.fnmatch(info["name"], x) for x in names):
                    continue

            # Case 2: skip it entirely!
            if skips and info["name"] in skips:
                continue

            existing_packages = self.get_existing_packages(
                info["name"], registry=registry
            )
            if pkg not in existing_packages:
                yield pkg, info

    def get_existing_tags(self, package, registry=None):
        """
        Get existing tags for a package name
        """
        registry = registry or self.registry

        global existing_tags_cache

        # These are empty packages that serve as helpers
        if package.startswith("_"):
            package = f"zzz{package}"

        if package in existing_tags_cache:
            return existing_tags_cache[package]

        # GitHub packages name
        gh_name = f"{registry}/{self.channel}/{self.subdir}/{package}"

        # We likely want this to raise an error if there is one.
        tags = oras.get_tags(gh_name).json().get("tags", [])
        existing_tags_cache[package] = tags
        return tags

    def get_existing_packages(self, package, registry=None):
        """
        Get existing package .tar.bz2 files for each tag
        """
        registry = registry or self.registry
        tags = self.get_existing_tags(registry, package)
        return set(f"{package}-{tag}.tar.bz2" for tag in tags)
