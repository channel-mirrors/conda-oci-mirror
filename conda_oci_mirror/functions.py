import json
import logging
import pathlib
from datetime import datetime
from pathlib import Path

from conda_oci_mirror.oci import OCI
from conda_oci_mirror.oras import ORAS, Layer

oci = OCI("https://ghcr.io", "MichaelKora")
all_sub_dirs = [
    "linux-64",
    "osx-64",
    "osx-arm64",
    "win-64",
    "linux-aarch64",
    "linux-ppc64le",
    "noarch",
]


def get_all_packages(repodata):
    found_packages = []
    for key in repodata["packages"]:
        found_packages.append(key)
    return found_packages


def compare_checksums(base, all_subdirs):
    differences = {"linux-64": [], "osx-64": [], "osx-arm64": [], "win-64": [], "linux-aarch64": [], "linux-ppc64le": [], "noarch": []}
    for subdir in all_subdirs:
        location = Path(base) / subdir
        if location.exists():
            repodata_checksums_path = location / "repodata_checksums.json"
            manifests_checksums_path = location / "manifest_checksums.json"

            with open(repodata_checksums_path) as fi:
                dict_repo_checksums = json.load(fi)

            with open(manifests_checksums_path) as fi:
                dict_manfst_checksums = json.load(fi)

            for key in dict_repo_checksums.keys():
                for sub_key in dict_repo_checksums[key].keys():
                    repo_check = dict_repo_checksums[key][sub_key]
                    manfst_check = dict_manfst_checksums[key][sub_key]
                    if repo_check != manfst_check:
                        differences[subdir].append(sub_key)

    return differences


def upload_index_json(global_index, channel, remote_loc):
    for key in global_index:
        # itterate throughevery pkg. e.g: zlib
        subdir = global_index["info"]["subdir"]
        index_file = {"info": {"subdir": {}}}
        index_file["info"]["subdir"] = subdir

        if key != "info":
            index_file["name"] = key

            # go through all the versions of a specific package. eg: zlib-12.0-1. zlib-12.0-2
            for pkg in global_index[key]:
                pkg_name = pkg["name"] + "-" + pkg["version"] + "-" + pkg["build"]
                index_file[pkg_name] = pkg

            dir_index = pathlib.Path(channel) / subdir / key
            dir_index.mkdir(mode=511, parents=True, exist_ok=True)

            json_object = json.dumps(index_file, indent=4)

            index_path = dir_index / "index.json"

            with open(index_path, "w") as write_file:
                json.dump(json_object, write_file)

            logging.warning("upload the index.json file...")
            upload_path = channel + "/" + subdir + "/" + index_file["name"] + "/index.json"

            now = datetime.now()
            tag = now.strftime("%d%m%Y%H%M%S")
            oras = ORAS(base_dir=dir_index)
            media_type = "application/json"
            layers = [Layer("index.json", media_type)]
            logging.warning(f"upload the json file for <<{key}>>")

            oras.push(
                f"{remote_loc}/{upload_path}", tag, layers
            )
            oras.push(
                f"{remote_loc}/{upload_path}", "latest", layers
            )

            logging.warning(f"index.json successfuly uploaded for {key}!")
            print(json_object)


def dict_is_empty(dict):
    for key in dict:
        if len(dict[key]) != 0:
            return False
    return True
