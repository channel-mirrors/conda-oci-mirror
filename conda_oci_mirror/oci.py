import tarfile
from io import BytesIO

import requests

from conda_oci_mirror import constants as C
from conda_oci_mirror.util import get_github_auth


class OCI:
    def __init__(self, location, user_or_org):
        self.location = location
        self.user_or_org = user_or_org
        self.session_map = {}

    def full_package(self, package):
        if package.startswith(self.user_or_org + "/"):
            return package
        return f"{self.user_or_org}/{package}"

    def oci_auth(self, package, scope="pull"):
        package = self.full_package(package)
        if package in self.session_map:
            return self.session_map[package]

        url = f"{self.location}/token?scope=repository:{package}:{scope}"
        auth = get_github_auth()

        r = requests.get(url, auth=auth)
        j = r.json()

        oci_session = requests.Session()
        oci_session.headers = {"Authorization": f'Bearer {j["token"]}'}
        self.session_map[package] = oci_session
        return oci_session

    def get_blob(self, package, digest, stream=False):
        package = self.full_package(package)

        url = f"{self.location}/v2/{package}/blobs/{digest}"
        oci_session = self.oci_auth(self.location, package)
        res = oci_session.get(url, stream=stream)
        return res

    def get_tags(self, package, n_tags=10_000, prev_last=None):
        package = self.full_package(package)
        print(f"Getting tags for {package}")
        url = f"{self.location}/v2/{package}/tags/list?n={n_tags}"
        if prev_last:
            url += "&last=prev_last"
        oci_session = self.oci_auth(self.location, package)

        tags = []
        link = True
        # get all tags using the pagination
        while link:
            res = oci_session.get(url)
            if not res.ok:
                return []

            if res.headers.get("Link"):
                link = res.headers.get("Link")
                assert link.endswith('; rel="next"')
                next_link = link.split("<")[len(link.split("<")) - 1].split(">")[0]
                url = self.location + next_link
            else:
                link = None

            tags += res.json()["tags"]

        return tags

    def get_manifest(self, package, tag):
        package = self.full_package(package)

        url = f"{self.location}/v2/{package}/manifests/{tag}"

        oci_session = self.oci_auth(package)
        headers = {"accept": "application/vnd.oci.image.manifest.v1+json"}
        r = oci_session.get(url, headers=headers)

        return r.json()

    def _find_digest(self, package, tag, media_type):
        package = self.full_package(package)

        url = f"{self.location}/v2/{package}/manifests/{tag}"

        oci_session = self.oci_auth(package)
        headers = {"accept": "application/vnd.oci.image.manifest.v1+json"}
        r = oci_session.get(url, headers=headers)

        j = r.json()
        for x in j["layers"]:
            if x["mediaType"] == media_type:
                digest = x["digest"]
        return digest

    def get_info(self, package, tag):
        digest = self._find_digest(package, tag, C.info_archive_media_type)
        res = self.get_blob(package, digest, stream=False)
        return tarfile.open(fileobj=BytesIO(res.content), mode="r:gz")

    def get_index_json(self, package, tag):
        digest = self._find_digest(package, tag, C.info_index_media_type)
        return self.get_blob(package, digest).json()
