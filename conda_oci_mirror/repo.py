# Packages and functions for them

import datetime
import distutils.version
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


# Mapping of extensions to media types
package_extensions = {
    "tar.bz2": defaults.package_tarbz2_media_type,
    "conda": defaults.package_conda_media_type,
}


class RepoData:
    """
    Courtesy wrapper to repodata to get packages, save, etc.
    """

    def __init__(self, filename=None, package_types=None):
        self.filename = filename

        # Control access to package types
        # We don't expose this yet, but eventually could
        self.package_types = package_types or ["packages", "packages.conda"]
        self.data = {package_type: {} for package_type in self.package_types}

        # Loading data here (or with load) over-rides the dummy empty data above
        if filename is not None:
            self.load(filename)

    def load(self, filename):
        """
        Load a filename into the repository data.
        """
        self.filename = os.path.abspath(filename)
        self.data = util.read_json(filename)

    @property
    def packages(self):
        """
        Yield all package types, the filename and info
        """
        for key in self.package_types:
            for package_file, info in self.data.get(key, {}).items():
                yield package_file, info

    @property
    def package_archives(self):
        """
        Return flat list of package archive file names
        """
        return [x[0] for x in list(self.packages)]

    def filtered_packages(self, names):
        """
        Yield a subset of packages in a set of names
        """
        # We can optionally accept a single string name
        if isinstance(names, str):
            names = [names]
        names = set(names)
        for package_file, info in self.packages:
            if info["name"] not in names:
                continue
            yield package_file, info

    def get_package_extension(self, pkg):
        """
        Get the package extension - sanity check it's conda or tar.bz2.
        """
        for ext in package_extensions:
            if pkg.endswith(ext):
                return ext
        raise ValueError(f"Unrecognized package extension for {pkg}")

    def get_package_mediatype(self, pkg):
        """
        Get the correct media type to ask for.
        """
        for ext, media_type in package_extensions.items():
            if pkg.endswith(ext):
                return media_type
        raise ValueError(f"Unrecognized package looking up media type {pkg}")

    @property
    def package_names(self):
        """
        Return unique set of package names
        """
        return set(x[1]["name"] for x in self.packages)

    def get_latest_tag(self, package):
        """
        Try to get the latest tag based on build number / version string.
        """
        # Subset to the info of those we care about
        subset = [info for _, info in self.filtered_packages(package)]

        # Cut out early if we don't have any packages
        if not subset:
            return

        # First, for each build, get the latest version based on build number
        packages = {}
        for entry in subset:
            if entry["version"] not in packages:
                packages[entry["version"]] = entry
                continue

            is_newer = (
                entry["build_number"] > packages[entry["version"]]["build_number"]
            )
            if entry["version"] in packages and is_newer:
                packages[entry["version"]] = entry

        # Find latest tag from set of highest build numbers
        tags = list(packages)
        tags.sort(key=distutils.version.StrictVersion)

        # The tag is technically the version + build number
        latest = packages[tags[-1]]
        return f"{latest['version']}-{latest['build']}"


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
    def patches(self):
        """
        Repository metadata plus packages yanked.
        """
        return os.path.join(self.cache_dir, "repodata_from_packages.json")

    @property
    def name(self):
        return os.path.join(self.channel, self.subdir)

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
        Get the pull package .conda or .tar.bz2 file
        """
        container = f"{self.registry}/{self.channel}/{self.subdir}/{package}"

        # Try for latest .conda version first
        res = None
        for _, media_type in package_extensions.items():
            res = oras.pull_by_media_type(container, self.cache_dir, media_type)
            if res:
                break

        # We cannot find either media type
        if not res:
            media_types = list(package_extensions.values())
            raise ValueError(
                f"Cannot pull {container} no media types {media_types} exist."
            )
        return res[0]

    def ensure_timestamp(self):
        """
        Ensure we have a timestamp when it was downloaded.
        """
        self.timestamp = datetime.datetime.now()

    def ensure_repodata(self):
        """
        Ensure respository metadata is freshly downloaded.
        """
        util.mkdir_p(os.path.dirname(self.repodata))
        logger.info(f"Downloading patches for {self.channel}/{self.subdir}")

        # The repodata is "patched" by this file: repodata_from_packages.json
        patches = requests.get(
            f"https://conda.anaconda.org/{self.channel}/{self.subdir}/repodata_from_packages.json",
            allow_redirects=True,
        )
        logger.info(f"Downloading fresh repodata for {self.channel}/{self.subdir}")
        repodata = requests.get(
            f"https://conda.anaconda.org/{self.channel}/{self.subdir}/repodata.json",
            allow_redirects=True,
        )

        # If we retrieve both succesfully, save both for later use
        if patches.status_code == 200:
            util.write_file(patches.text, self.patches)
        if repodata.status_code == 200:
            util.write_file(repodata.text, self.repodata)
        self.ensure_timestamp()

    def upload(self, root, registry=None):
        """
        Push the repodata.json to a named registry from root context.
        """
        registry = registry or self.registry
        self.ensure_repodata()
        pushes = []

        # title is used for archive name (path extracted to) so relative to root
        # note that we upload repodata.json here, not the one with yanked packages
        title = os.path.relpath(self.repodata, root)

        # Push should be relative to cache context
        uri = f"{registry}/{self.channel}/{self.subdir}/repodata.json"

        # Don't be pushy now, or actually, do :)
        pusher = Pusher(root, self.timestamp)
        pusher.add_layer(self.repodata, defaults.repodata_media_type_v1, title)

        # Push for a tag for the date, and latest
        for tag in pusher.created_at, "latest":
            logger.info(f"  pushing tag {tag}")
            pushes.append(pusher.push(f"{uri}:{tag}"))

        # Return pushes
        return pushes

    def load_repodata(self, include_yanked=True):
        """
        Load repository data (json)

        We always retrieve it fresh.
        """
        self.ensure_repodata()
        if include_yanked and not os.path.exists(self.patches):
            logger.warning(
                "Repodata from packages (with yanked packages) does not exist, falling back to repodata.json"
            )
        elif include_yanked:
            return RepoData(self.patches)
        return RepoData(self.repodata)

    def find_packages(self, names=None, skips=None, registry=None, include_yanked=True):
        """
        Given loaded repository data, find packages of interest
        """
        registry = registry or self.registry
        skips = skips or []
        repodata = self.load_repodata(include_yanked)

        # Look through package info for conda and regular packages
        # These don't overlap, version wise, so it's safe to do.
        for pkg, info in repodata.packages:
            # Case 1: we are given packages to filter to
            if names:
                if not any(fnmatch.fnmatch(info["name"], x) for x in names):
                    continue

            # Case 2: skip it entirely!
            if skips and info["name"] in skips:
                continue

            # Existing packages for this will depend on the extension
            existing_packages = self.get_existing_packages(
                info["name"],
                registry=registry,
                package_ext=repodata.get_package_extension(pkg),
            )

            # This check includes extension, so shouldn't be an issue
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

    def get_existing_packages(self, package, registry=None, package_ext="conda"):
        """
        Get existing package files files for each tag

        The extension depends on whether the package is the new format
        (.conda) vs old (tar.bz2)
        """
        registry = registry or self.registry
        tags = self.get_existing_tags(package, registry=registry)
        return set(f"{package}-{tag}.{package_ext}" for tag in tags)
