import os
import pathlib

info_archive_media_type = "application/vnd.conda.info.v1.tar+gzip"
info_index_media_type = "application/vnd.conda.info.index.v1+json"
package_tarbz2_media_type = "application/vnd.conda.package.v1"
package_conda_media_type = "application/vnd.conda.package.v2"
repodata_media_type_v1 = "application/vnd.conda.repodata.v1+json"

CACHE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__))) / "cache"

# Default subdirectories in a conda package
DEFAULT_SUBDIRS = [
    "linux-64",
    "osx-64",
    "osx-arm64",
    "win-64",
    "linux-aarch64",
    "linux-ppc64le",
    "noarch",
]

# Package urls, etc.
forbidden_package_url = "https://raw.githubusercontent.com/conda-forge/repodata-tools/main/repodata_tools/metadata.json"
