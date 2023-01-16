# Packages and functions for them

import datetime
import fnmatch
import logging
import os

import requests

import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.util as util
from conda_oci_mirror.oras import Pusher, oras

logger = logging.getLogger(__name__)

# This is shared between PackageRepo instances
existing_tags_cache = {}


class PackageRepo:
    """
    A package repository manages a conda package repository.
    """

    def __init__(self, channel, subdir, cache_dir):
        self.channel = channel
        self.subdir = subdir
        self.cache_dir = cache_dir or defaults.CACHE_DIR
        self.timestamp = None

    @property
    def repodata(self):
        return os.path.join(self.cache_dir, "repodata.json")

    @property
    def name(self):
        return os.path.join(self.channel, self.subdir)

    def exists(self):
        return os.path.exists(self.repodata)

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

    def upload(self, registry, root):
        """
        Push the repodata.json to a named registry from root context.
        """
        repodata = self.get_repodata()

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
            pusher.push(f"{uri}:{tag}")

    def load_repodata(self):
        """
        Load repository data (json)
        """
        if not self.exists():
            self.get_repodata()
        return util.read_json(self.repodata)

    def find_packages(self, namespace, names=None, skips=None):
        """
        Given loaded repository data, find packages of interest
        """
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

            existing_packages = self.get_existing_packages(info["name"], namespace)
            if pkg not in existing_packages:
                yield pkg, info

    def get_existing_tags(self, package, namespace):
        """
        Get existing tags for a package name
        """
        global existing_tags_cache

        if package.startswith("_"):
            package = f"zzz{package}"

        if package in existing_tags_cache:
            return existing_tags_cache[package]

        # GitHub packages name
        gh_name = f"{namespace}/{self.channel}/{self.subdir}/{package}"

        # We likely want this to raise an error if there is one.
        tags = oras.get_tags(gh_name).json().get("tags", [])
        existing_tags_cache[package] = tags
        return tags

    def get_existing_packages(self, namespace, package):
        """
        Get existing package .tar.bz2 files for each tag
        """
        tags = self.get_existing_tags(namespace, package)
        return set(f"{package}-{tag}.tar.bz2" for tag in tags)
