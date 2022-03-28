import os
import pathlib

info_archive_media_type = "application/vnd.conda.info.v1.tar+gzip"
info_index_media_type = "application/vnd.conda.info.index.v1+json"
package_tarbz2_media_type = "application/vnd.conda.package.v1"
package_conda_media_type = "application/vnd.conda.package.v2"

CACHE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__))) / "cache"
