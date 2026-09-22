# Packages and functions for them

import datetime
import os
import tarfile

import requests
import zstandard as zstd
from rattler import Version

import conda_oci_mirror.decorators as decorators
import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.util as util
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import Pusher, get_oras_client, registry_name
from conda_oci_mirror.package import (
    matches_package,
    package_reference,
    reverse_version_build_tag,
)

# Mapping of extensions to media types
package_extensions = {
    "conda": defaults.package_conda_media_type,
    "tar.bz2": defaults.package_tarbz2_media_type,
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

    def latest_packages(self, names=None):
        """Select the newest version/build per name and archive format in one scan."""
        latest = {}
        versions = {}
        for filename, info in self.packages:
            if not matches_package(info["name"], names):
                continue
            version = info["version"]
            if version not in versions:
                versions[version] = Version(version)
            rank = (versions[version], info["build_number"])
            key = (info["name"], self.get_package_extension(filename))
            # Preserve the first record when version/build numbers tie, as before.
            if key not in latest or rank > latest[key][0]:
                latest[key] = (rank, filename, info)
        for _, filename, info in latest.values():
            yield filename, info

    def get_latest_tag(self, package, package_ext=None):
        """Return the newest conda version/build, optionally for one archive format."""
        candidates = (
            info
            for filename, info in self.latest_packages([package])
            if package_ext in (None, self.get_package_extension(filename))
        )
        latest = max(
            candidates,
            key=lambda info: (Version(info["version"]), info["build_number"]),
            default=None,
        )
        if latest is not None:
            return f"{latest['version']}-{latest['build']}"


class PackageRepo:
    """
    A package repository manages a conda package repository.

    Note that a PackageRepo can be used as the previous "SubdirAccessor"
    """

    def __init__(self, channel, subdir, cache_dir, registry=None, client=None):
        self.channel = channel
        self.subdir = subdir
        self.cache_dir = cache_dir or defaults.CACHE_DIR
        self.timestamp = None
        self._existing_tags = {}
        self._existing_manifests = {}
        self.new_archives = set()

        # Can be over-ridden by upload/tags/packages functions if desired
        self.client = client or get_oras_client(registry)
        self.registry = registry_name(registry)

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
        container = (
            f"{self.registry}/{self.channel}/{self.subdir}/{package_reference(package)}"
        )

        # We pull to the higher up cache directory, which should extract to cache
        # E.g., '/tmp/pytest-of-vanessa/pytest-19/test_package_repo_linux_64_0/cache
        # and we extract '<ditto>/cache/zlib-1.2.11-0/info/index.json
        res = self.client.pull_by_media_type(
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
        container = (
            f"{self.registry}/{self.channel}/{self.subdir}/{package_reference(package)}"
        )
        res = self.client.pull_by_media_type(
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
        container = (
            f"{self.registry}/{self.channel}/{self.subdir}/{package_reference(package)}"
        )

        # Try for latest .conda version first
        res = None
        for _, media_type in package_extensions.items():
            res = self.client.pull_by_media_type(container, self.cache_dir, media_type)
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
        self.timestamp = None
        util.mkdir_p(os.path.dirname(self.repodata))
        logger.info(f"Downloading patches for {self.channel}/{self.subdir}")

        # The repodata is "patched" by this file: repodata_from_packages.json
        patches = requests.get(
            f"https://conda.anaconda.org/{self.channel}/{self.subdir}/repodata_from_packages.json",
            allow_redirects=True,
            timeout=60,
        )
        logger.info(f"Downloading fresh repodata for {self.channel}/{self.subdir}")
        repodata = requests.get(
            f"https://conda.anaconda.org/{self.channel}/{self.subdir}/repodata.json",
            allow_redirects=True,
            timeout=60,
        )

        # Never publish stale metadata after a failed or invalid response.
        repodata.raise_for_status()
        repodata.json()
        if patches.status_code == 404:
            if os.path.exists(self.patches):
                os.remove(self.patches)
        else:
            patches.raise_for_status()
            patches.json()
            util.write_file(patches.text, self.patches)
        util.write_file(repodata.text, self.repodata)
        self.ensure_timestamp()

    def upload(self, root, registry=None):
        """
        Publish the last loaded snapshot, downloading it if none has been loaded.
        """
        registry = registry or self.registry
        client = self.client if registry == self.registry else get_oras_client(registry)
        registry = registry_name(registry)
        if self.timestamp is None:
            self.ensure_repodata()
        pushes = []

        # title is used for archive name (path extracted to) so relative to root
        # note that we upload repodata.json here, not the one with yanked packages
        title = os.path.relpath(self.repodata, root)

        # Push should be relative to cache context
        uri = f"{registry}/{self.channel}/{self.subdir}/repodata.json"

        # Don't be pushy now, or actually, do :)
        pusher = Pusher(root, self.timestamp, client=client)
        pusher.add_layer(self.repodata, defaults.repodata_media_type_v1, title)

        # compress repodata with zstd
        compressed = self.compress_repodata()
        pusher.add_layer(
            compressed, defaults.repodata_media_type_v1_zst, title + ".zst"
        )

        # Push for a tag for the date, and latest
        for tag in pusher.created_at, "latest":
            logger.info(f"  pushing tag {tag}")
            pushes.append(pusher.push(f"{uri}:{tag}"))

        # Return pushes
        return pushes

    def compress_repodata(self):
        # Create a temporary file
        zst_file = self.repodata + ".zst"

        # Initialize Zstandard compressor
        cctx = zstd.ZstdCompressor(level=15)

        with open(self.repodata, "rb") as source_file:
            # Read the content of the source file
            data = source_file.read()

            # Compress the data
            compressed_data = cctx.compress(data)

            with open(zst_file, "wb") as fout:
                fout.write(compressed_data)

        # Return the path to the temporary file
        return zst_file

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
        self._existing_tags.clear()
        self._existing_manifests.clear()
        self.new_archives.clear()
        repodata = self.load_repodata(include_yanked)

        # Most builds have one format; inspect layers when both formats are listed.
        for pkg, info in repodata.packages:
            # Case 1: we are given packages to filter to
            if not matches_package(info["name"], names):
                continue

            # Case 2: skip it entirely!
            if skips and info["name"] in skips:
                continue

            ext = repodata.get_package_extension(pkg)
            # ponytail: unique upstream formats use tag presence; full audits must
            # verify layers with get_existing_packages(verify_media_type=True).
            existing_packages = self.get_existing_packages(
                info["name"],
                registry=registry,
                package_ext=ext,
                verify_media_type=False,
            )
            exists = pkg in existing_packages
            if not exists:
                self.new_archives.add(pkg)
            other_key, other_ext = (
                ("packages", "tar.bz2")
                if ext == "conda"
                else ("packages.conda", "conda")
            )
            other_file = pkg[: -(len(ext) + 1)] + "." + other_ext
            if exists and other_file in repodata.data.get(other_key, {}):
                start = len(info["name"]) + 1
                end = -(len(ext) + 1)
                tag = pkg[start:end]
                exists = self.has_package_format(info["name"], tag, ext, registry)
            if not exists:
                logger.info(f"Adding {pkg} to queue")
                yield pkg, info

    def get_existing_tags(self, package, registry=None):
        """
        Get existing tags for a package name
        """
        registry = registry or self.registry
        client = self.client if registry == self.registry else get_oras_client(registry)
        gh_name = f"{registry_name(registry)}/{self.channel}/{self.subdir}/{package_reference(package)}"
        key = (client.prefix, gh_name)
        if key not in self._existing_tags:
            try:
                tags = client.get_tags(gh_name, N=100_000_000)
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 404:
                    raise
                tags = []
            logger.info(f"Found {len(tags)} tags for {gh_name}")
            self._existing_tags[key] = [reverse_version_build_tag(t) for t in tags]
        return self._existing_tags[key]

    def has_package_format(self, package, tag, package_ext, registry=None):
        """Inspect one tag's descriptors without downloading package blobs."""
        registry = registry or self.registry
        client = self.client if registry == self.registry else get_oras_client(registry)
        uri = f"{registry_name(registry)}/{self.channel}/{self.subdir}/{package_reference(package, tag)}"
        key = (client.prefix, uri)
        if key not in self._existing_manifests:
            self._existing_manifests[key] = client.get_optional_manifest(uri)
        manifest = self._existing_manifests[key] or {}
        return any(
            layer["mediaType"] == package_extensions[package_ext]
            for layer in manifest.get("layers", [])
        )

    def get_existing_packages(
        self, package, registry=None, package_ext="conda", verify_media_type=True
    ):
        """
        Get existing archives of one format, checking manifest layers by default.

        verify_media_type=False is a tag-only estimate used by the mirror's fast
        path. Full audits should keep the default; descriptors are cached per scan.
        """
        registry = registry or self.registry
        tags = self.get_existing_tags(package, registry=registry)
        if verify_media_type:
            tags = [
                tag
                for tag in tags
                if self.has_package_format(package, tag, package_ext, registry)
            ]
        return set(f"{package}-{tag}.{package_ext}" for tag in tags)
