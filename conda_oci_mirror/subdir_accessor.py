import json
import pathlib
import tarfile

from constants import (
    info_archive_media_type,
    info_index_media_type,
    package_tarbz2_media_type,
)
from oras import ORAS


class SubdirAccessor:
    def __init__(self, location, subdir, base_dir="."):
        self.loc = location
        self.subdir = subdir
        self.oras = ORAS(base_dir=base_dir)

    def get_index_json(self, package_name):
        self.oras.pull(self.loc, self.subdir, package_name, info_index_media_type)
        with open(pathlib.Path(package_name) / "info" / "index.json") as fi:
            return json.load(fi)

    def get_info(self, package_name):
        self.oras.pull(self.loc, self.subdir, package_name, info_archive_media_type)
        return tarfile.open(pathlib.Path(package_name) / "info.tar.gz", "r:gz")

    def get_package(self, package_name):
        self.oras.pull(self.loc, self.subdir, package_name, package_tarbz2_media_type)
        return package_name + ".tar.bz2"
