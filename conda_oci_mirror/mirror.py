import datetime
import os
import pathlib
import shutil
import subprocess

import requests

import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.package as pkg
import conda_oci_mirror.repo as repository
import conda_oci_mirror.tasks as tasks
import conda_oci_mirror.util as util
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import oras


def get_forbidden_packages():
    """
    Get listing of undistributable packages from conda.
    """
    response = requests.get(defaults.forbidden_package_url)
    if response.status_code != 200:
        raise ValueError(
            f"Cannot retrieve forbidden packages from {defaults.forbidden_package_url}"
        )
    return response.json()["undistributable"]


def conda_index(cache_dir):
    """
    conda index to create updated repodata.json
    """
    subprocess.check_output(["conda", "index", str(cache_dir)])


class Mirror:
    """
    A Mirror represents a conda Mirror with an associated registry.
    """

    def __init__(
        self,
        channels,
        subdirs,
        packages,
        namespace=None,
        host=None,
        cache_dir=None,
        quiet=False,
    ):
        self.channels = channels
        self.subdirs = subdirs
        self.packages = packages

        # Default mirrors are here
        self.namespace = namespace or "channel-mirrors"

        # Registry host, defaults to GitHub
        self.host = host or "ghcr.io"
        self.cache_dir = os.path.abspath(cache_dir or defaults.CACHE_DIR)
        self.quiet = quiet
        self.announce()

        # Ensure the oras registry is set to insecure or not based on host
        global oras
        oras.prefix = "http" if host.startswith("http://") else "https"

        # Set listing of (undistributable) packages to skip
        self.skip_packages = (
            get_forbidden_packages() if "conda-forge" in channels else None
        )

    @property
    def registry(self):
        """
        Assemble the host and namespace (user or org) into a registry URI
        """
        if not self.namespace or not self.host:
            raise ValueError("Both a host and namespace are required.")
        return f"{self.host}/{self.namespace}"

    def announce(self):
        """
        Show metadata about the mirror setup
        """
        util.print_item("Using cache dir:", self.cache_dir)
        util.print_item("Channels  :", self.channels)
        util.print_item("  Subdirs :", self.subdirs)
        util.print_item("  Packages:", self.packages)

    def update(self, dry_run=False):
        """
        Update from a conda mirror - akin to a pull and a push.
        """
        util.print_item("To: ", self.registry)

        # Create a task runner (defaults to 4 processes)
        runner = tasks.TaskRunner()

        # If they think they are pushing but no auth, they are not :)
        if not oras.has_auth and dry_run is False:
            logger.warning(
                "ORAS is not authenticated, if you registry requires auth this will not work"
            )

        for channel, subdir, cache_dir in self.iter_channels():
            repo = repository.PackageRepo(channel, subdir, cache_dir)

            # Run filter based on packages we are looking for, and forbidden
            for package, info in repo.find_packages(
                self.registry, self.packages, self.skip_packages
            ):

                # Add the new tasks to be run by the runner
                # This will get mapped into a Package instance to interact with
                runner.add_task(
                    tasks.Task(
                        channel,
                        subdir,
                        package,
                        cache_dir,
                        self.registry,
                        dry_run=dry_run,
                        info=info,
                    )
                )

            # We can't actually push without auth
            if dry_run:
                logger.info(
                    f"Would push {repo.name} to {self.registry}, skipping for dry-run."
                )
                continue

            # Use oras to push updated repository data and run tasks
            repo.upload(self.registry, cache_dir)

        # Once we get here, run all tasks
        runner.run()

    def iter_channels(self):
        """
        yield groups of channels, subdir, and cache directories.
        """
        for channel in self.channels:
            for subdir in self.subdirs:
                cache_dir = os.path.join(self.cache_dir, channel, subdir)
                yield channel, subdir, cache_dir

    def pull_latest(self, dry_run=False):
        """
        Pull latest packages from a location (the GitHub user) to a local cache.
        """
        util.print_item("From: ", self.registry)
        util.print_item("  To: ", self.cache_dir)

        for channel, subdir, cache_dir in self.iter_channels():

            # Note that the original channel is relevant for a mirror
            uri = f"{self.registry}/{channel}/{subdir}/repodata.json:latest"

            try:
                # Retrieve a path to the index_file
                index_file = oras.pull_by_media_type(
                    uri, cache_dir, defaults.repodata_media_type_v1
                )[0]
                repodata = util.read_json(index_file)
                packages = set([p["name"] for _, p in repodata["packages"].items()])
                logger.info(f"Found len(packages) packages from {uri}")

            except Exception as e:
                packages = set()
                logger.warning(f"Issue retriving uri: {uri}: {e}")

            for package in packages:

                # Skip those that aren't desired if a filter is given
                if self.packages and package not in self.packages:
                    continue

                uri = f"{self.registry}/{channel}/{subdir}/{package}:latest"

                # Dry run don't actually do it
                if dry_run:
                    logger.info(f"Would be pulling {package}, but dry-run is set.")
                    continue

                # Not every package is guaranteed to exist
                try:
                    oras.pull_by_media_type(
                        uri, cache_dir, defaults.package_tarbz2_media_type
                    )
                except Exception as e:
                    logger.warning(f"Cannot pull package {package}: {e}")

    def push_new(self, dry_run=False):
        """
        Push new packages to the remote.
        """
        util.print_item("From: ", self.cache_dir)
        util.print_item("  To: ", self.registry)

        for channel, subdir, cache_dir in self.iter_channels():

            # The channel cache is one level up from our subdir cache
            channel_root = os.path.dirname(cache_dir)

            # Backup the original repository data so we can index and replace it
            backup_repodata = os.path.join(cache_dir, "original_repodata.json")
            orig_repodata = os.path.join(cache_dir, "repodata.json")

            # If we already have repository data, make a copy
            if os.path.exists(orig_repodata):
                shutil.copyfile(orig_repodata, backup_repodata)

            # This nukes the repodata.json
            conda_index(channel_root)

            # Create new repodata or load existing from backup (before nuke)
            if os.path.exists(backup_repodata):
                repodata = util.read_json(backup_repodata)
            else:
                repodata = {"packages": []}
            files = list(pathlib.Path(cache_dir).rglob("*.tar.bz2"))
            new_packages = [f for f in files if f.name not in repodata["packages"]]
            logger.info(f"Found {len(new_packages)} new packages")

            # Push with an updated timestamp
            timestamp = datetime.datetime.now().strftime("%Y.%m.%d.%H%M%S")

            # Upload new packages
            for package_name in new_packages:
                package = pkg.Package(
                    channel,
                    subdir,
                    package_name,
                    cache_dir,
                    namespace=self.registry,
                    existing_file=str(package_name),
                )
                package.upload(dry_run, timestamp)

            # If we cleanup, remove repodata.json and replace back with original
            os.remove(orig_repodata)
            if os.path.exists(backup_repodata):
                shutil.move(backup_repodata, orig_repodata)
