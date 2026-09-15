# Packages and functions for them

import datetime
import fnmatch
import os
import tarfile
from urllib.parse import urljoin

import msgpack
import packaging.version
import requests
import zstandard as zstd

import conda_oci_mirror.decorators as decorators
import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.util as util
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import Pusher, oras
from conda_oci_mirror.package import reverse_version_build_tag

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
        tags.sort(key=packaging.version.Version)

        # The tag is technically the version + build number
        latest = packages[tags[-1]]
        return f"{latest['version']}-{latest['build']}"


class PackageRepo:
    """
    A package repository manages a conda package repository.

    Note that a PackageRepo can be used as the previous "SubdirAccessor"
    """

    def __init__(self, channel, subdir, cache_dir, registry=None, mirror_shards=True):
        self.channel = channel
        self.subdir = subdir
        self.cache_dir = cache_dir or defaults.CACHE_DIR
        self.timestamp = None
        self.mirror_shards = mirror_shards

        # Can be over-ridden by upload/tags/packages functions if desired
        self.registry = registry

        # Should the registry requests use http or https?
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
    def shard_index(self):
        return os.path.join(self.cache_dir, "repodata_shards.msgpack.zst")

    @property
    def shards_dir(self):
        return os.path.join(self.cache_dir, "shards")

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
        self.ensure_sharded_repodata()
        self.ensure_timestamp()

    def ensure_sharded_repodata(self):
        """Download the shard index and every content-addressed shard it references."""
        if not self.mirror_shards:
            return []

        base_url = f"https://conda.anaconda.org/{self.channel}/{self.subdir}"
        response = requests.get(f"{base_url}/repodata_shards.msgpack.zst")
        if response.status_code != 200:
            if os.path.exists(self.shard_index):
                os.remove(self.shard_index)
            return []

        util.mkdir_p(self.shards_dir)
        index = msgpack.unpackb(
            zstd.ZstdDecompressor().decompress(response.content), raw=False
        )
        source_shards_url = urljoin(
            f"{base_url}/", index["info"].get("shards_base_url", "")
        )
        digests = sorted(
            digest if isinstance(digest, str) else bytes(digest).hex()
            for digest in set(index["shards"].values())
        )
        for digest in digests:
            shard_path = os.path.join(self.shards_dir, f"{digest}.msgpack.zst")
            if os.path.exists(shard_path) and util.sha256sum(shard_path) == digest:
                continue
            shard = requests.get(
                urljoin(f"{source_shards_url.rstrip('/')}/", f"{digest}.msgpack.zst")
            )
            if shard.status_code != 200:
                raise ValueError(f"Cannot retrieve repodata shard {digest}")
            with open(shard_path, "wb") as shard_file:
                shard_file.write(shard.content)
            if util.sha256sum(shard_path) != digest:
                raise ValueError(f"Repodata shard {digest} has an unexpected digest")

        # OCI uses a single `shards` repository instead of mirroring the source
        # server's physical paths. Package and shard URLs must remain relative
        # to the OCI channel selected by rattler.
        index["info"]["base_url"] = ""
        index["info"]["shards_base_url"] = "./shards/"
        with open(self.shard_index, "wb") as index_file:
            index_file.write(
                zstd.ZstdCompressor().compress(msgpack.packb(index, use_bin_type=True))
            )
        return digests

    def upload_shards(self, root, registry):
        """Push immutable shards before publishing the index that references them."""
        if not self.mirror_shards or not os.path.exists(self.shard_index):
            return []

        with open(self.shard_index, "rb") as index_file:
            index = msgpack.unpackb(
                zstd.ZstdDecompressor().decompress(index_file.read()), raw=False
            )
        digests = sorted(
            digest if isinstance(digest, str) else bytes(digest).hex()
            for digest in set(index["shards"].values())
        )
        pushes = []
        uri = f"{registry}/{self.channel}/{self.subdir}/shards"
        try:
            existing_digests = set(oras.get_tags(uri, N=100_000_000))
        except (TypeError, ValueError):
            existing_digests = set()
        # ponytail: shard transfer is serial; use TaskRunner if initial mirror
        # throughput becomes a problem.
        for digest in digests:
            if digest in existing_digests:
                continue
            shard_path = os.path.join(self.shards_dir, f"{digest}.msgpack.zst")
            pusher = Pusher(root, self.timestamp)
            pusher.add_layer(
                shard_path,
                defaults.repodata_shard_media_type_v1,
                os.path.relpath(shard_path, root),
            )
            pushes.append(pusher.push(f"{uri}:{digest}"))
        return pushes

    def upload(self, root, registry=None):
        """
        Push the repodata.json to a named registry from root context.
        """
        registry = registry or self.registry
        self.ensure_repodata()
        pushes = self.upload_shards(root, registry)

        # title is used for archive name (path extracted to) so relative to root
        # note that we upload repodata.json here, not the one with yanked packages
        title = os.path.relpath(self.repodata, root)

        # Push should be relative to cache context
        uri = f"{registry}/{self.channel}/{self.subdir}/repodata.json"

        # Don't be pushy now, or actually, do :)
        pusher = Pusher(root, self.timestamp)
        pusher.add_layer(self.repodata, defaults.repodata_media_type_v1, title)

        # compress repodata with zstd
        compressed = self.compress_repodata()
        pusher.add_layer(compressed, defaults.repodata_media_type_v1_zst, title)

        if self.mirror_shards and os.path.exists(self.shard_index):
            pusher.add_layer(
                self.shard_index,
                defaults.repodata_shards_media_type_v1,
                os.path.relpath(self.shard_index, root),
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
            try:
                existing_packages = self.get_existing_packages(
                    info["name"],
                    registry=registry,
                    package_ext=repodata.get_package_extension(pkg),
                )
            except (ValueError, TypeError):
                logger.warning(f"Package not yet in registry ({pkg})")
                existing_packages = set()

            # This check includes extension, so shouldn't be an issue
            if pkg not in existing_packages:
                logger.info(f"Adding {pkg} to queue")
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
        tags = oras.get_tags(gh_name, N=100_000_000)
        logger.info(f"Found {len(tags)} tags for {gh_name}")
        existing_tags_cache[package] = [reverse_version_build_tag(t) for t in tags]
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
