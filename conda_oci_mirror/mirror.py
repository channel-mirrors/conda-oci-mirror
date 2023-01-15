import logging
import os

import requests

import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.repo as repository
import conda_oci_mirror.tasks as tasks
import conda_oci_mirror.util as util
from conda_oci_mirror.oras import oras

logger = logging.getLogger(__name__)


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
        util.print_item("Mirroring :", self.channels)
        util.print_item("  Subdirs :", self.subdirs)
        util.print_item("  Packages:", self.packages)
        if self.registry:
            util.print_item("To: ", self.registry)

    def update(self, dry_run=False):
        """
        Update from a conda mirror.
        """
        # Create a task runner (defaults to 4 processes)
        runner = tasks.TaskRunner()

        # If they think they are pushing but no auth, they are not :)
        if not oras.has_auth and dry_run is False:
            logger.warning("ORAS is not authenticated, this will be a dry run.")
            dry_run = True

        for channel, subdir, cache_dir in self.iter_packages():
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
                        info,
                        cache_dir,
                        self.registry,
                        dry_run=dry_run,
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

    def iter_packages(self):
        """
        yield groups of channels, subdir, and cache directories.
        """
        for channel in self.channels:
            for subdir in self.subdirs:
                cache_dir = os.path.join(self.cache_dir, channel, subdir)
                yield channel, subdir, cache_dir
