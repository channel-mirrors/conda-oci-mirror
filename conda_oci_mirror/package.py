# Packages and functions for them

import hashlib
import json
import os
import pathlib
import platform
import shutil
import tempfile

import requests
from conda_package_handling import api

import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.util as util
from conda_oci_mirror.decorators import classretry, retry
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import Pusher


def check_checksum(path, package_dict):
    """
    Ensure the checksum (if exists) matches
    """
    if "sha256" in package_dict:
        hash_func = hashlib.sha256()
        expected = package_dict["sha256"]
    elif "md5" in package_dict:
        hash_func = hashlib.md5()
        expected = package_dict["md5"]
    else:
        logger.warning("NO HASHES FOUND!")
        return True

    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)

    if hash_func.hexdigest() != expected:
        return False
    else:
        return True


@retry(attempts=5, timeout=2)
def download_file(url, dest, checksum_content=None, chunk_size=8192):
    """
    Stream download a file!
    """
    with requests.get(url, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    # If we aren't given a checksum, we're done!
    if not checksum_content:
        return dest

    # Do a checksum validation if given one.
    if check_checksum(dest, checksum_content) is False:
        if os.path.exists(dest):
            os.remove(dest)
        raise RuntimeError("checksums wrong")

    return dest


class Package:
    def __init__(
        self,
        channel,
        subdir,
        package,
        cache_dir,
        registry,
        info=None,
        existing_file=None,
        timestamp=None,
    ):
        """
        Info is only required if the file does not exist yet.
        """
        self.channel = channel
        self.subdir = subdir
        self.package = package
        self.package_info = info
        self.cache_dir = cache_dir
        self.registry = registry
        self._package_name = None
        self.file = existing_file
        self.timestamp = timestamp

    def ensure_file(self):
        """
        Ensure self.file has been downloaded, and exists.
        """
        # This will retry 5 times and ensure the checksums match
        if not self.file or not os.path.exists(self.file):
            url = f"https://conda.anaconda.org/{self.channel}/{self.subdir}/{self.package}"
            dest = os.path.join(self.cache_dir, self.package)

            # Download the file and return it's path (default is to stream)
            self.file = download_file(url, dest, self.package_info)

    @property
    def package_name(self):
        """
        Get the package name based on the extension
        """
        if self._package_name is not None:
            return self._package_name

        name = pathlib.Path(self.file).name
        for ext in [".tar.bz2", ".conda"]:
            if name.endswith(ext):
                self._package_name = name[: -len(ext)]
                return self._package_name
        raise RuntimeError("Cannot decipher package type")

    @property
    def package_name_bare(self):
        """
        Package name without the version.
        """
        return self.package_name.rsplit("-", 2)[0]

    @property
    def tag(self):
        return "-".join(self.package_name.rsplit("-", 2)[1:])

    @property
    def version_build_tag(self):
        return (
            self.tag.replace("+", "__p__").replace("!", "__e__").replace("=", "__eq__")
        )

    @property
    def reverse_version_build_tag(self):
        return (
            self.tag.replace("__p__", "+").replace("__e__", "!").replace("__eq__", "=")
        )

    def delete(self):
        if self.file and os.path.exists(self.file):
            os.remove(self.file)

    def prepare_metadata(self, staging_dir):
        """
        Prepare package metadata for upload
        """
        dest_dir = os.path.join(staging_dir, self.package_name)
        util.mkdir_p(dest_dir)

        # Extract to another temporary location
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.debug(f"Extracting {self.file} to {temp_dir}")
            api.extract(self.file, temp_dir, components=["info"])

            index_json = os.path.join(temp_dir, "info", "index.json")
            info_archive = os.path.join(temp_dir, "info.tar.gz")
            util.compress_folder(
                os.path.join(temp_dir, "info"), os.path.join(temp_dir, "info.tar.gz")
            )
            util.mkdir_p(os.path.join(dest_dir, "info"))
            shutil.copy(info_archive, os.path.join(dest_dir, "info.tar.gz"))
            shutil.copy(index_json, os.path.join(dest_dir, "info", "index.json"))

    @classretry
    def upload(self, dry_run=False, extra_tags=None, timestamp=None):
        """
        Upload a conda package archive.
        """
        # Optionally honor the timestamp provided by the package
        timestamp = timestamp or self.timestamp
        extra_tags = extra_tags or []

        # Return list of items we uploaded
        items = []

        # If we are not given an iterable
        if not isinstance(extra_tags, (list, set, tuple)):
            extra_tags = set([extra_tags])

        with tempfile.TemporaryDirectory() as staging_dir:
            pusher = Pusher(staging_dir, timestamp=timestamp)
            upload_files_path = pathlib.Path(staging_dir)
            shutil.copy(self.file, staging_dir)

            # Prepare metadata in same staging directory
            self.prepare_metadata(staging_dir)

            # The new archive is the old filename in the new directory
            archive = os.path.join(staging_dir, os.path.basename(self.file))

            # title is used for archive name (path extracted to) so relative to root
            title = os.path.relpath(archive, staging_dir)
            media_type = (
                defaults.package_tarbz2_media_type
                if archive.endswith("tar.bz2")
                else defaults.package_conda_media_type
            )

            # Annotations are only included with tar.bz2
            annotations = None
            if media_type == defaults.package_conda_media_type:
                annotations = {"org.conda.md5": util.md5sum(archive)}
            pusher.add_layer(archive, media_type, title, annotations)

            # creation of info.tar.gz _does not yet work on windows_ properly...
            if platform.system() != "Windows":
                pusher.add_layer(
                    f"{self.package_name}/info.tar.gz", defaults.info_archive_media_type
                )

            # We always add this layer regardless of platform
            pusher.add_layer(
                f"{self.package_name}/info/index.json", defaults.info_index_media_type
            )

            if dry_run:
                logger.info(
                    f"Would be pushing to {self.registry}:{json.dumps(pusher.layers, indent=4)}"
                )
                return items

            name = self.package_name_bare
            version_and_build = self.tag
            index_file = os.path.join(
                upload_files_path, self.package_name, "info", "index.json"
            )
            index = util.read_json(index_file)

            # The index must contain the subdirectory
            subdir = index.get("subdir")
            if not subdir:
                logger.error(
                    f"info.json for {name}@{version_and_build} doesn't contain subdir!"
                )
                return

            # Is this a private or similar package? (not sure what this is doing)
            if name.startswith("_"):
                name = f"zzz{name}"

            # Push main tag and extras
            uri = f"{self.registry}/{self.channel}/{self.subdir}/{name}"
            for tag in [version_and_build] + list(extra_tags):
                items.append(pusher.push(f"{uri}:{tag}"))
            return items
