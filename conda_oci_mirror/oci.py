import os

import requests


class OCI:
    def __init__(self, location):
        self.location = location
        self.session_map = {}

    def oci_auth(self, package, scope="pull"):
        if package in self.session_map:
            return self.session_map[package]

        url = f"{self.location}/token?scope=repository:{package}:{scope}"
        auth = None
        if os.environ.get("GHA_PAT"):
            auth = (os.environ["GHA_USER"], os.environ["GHA_PAT"])
        r = requests.get(url, auth=auth)
        j = r.json()

        oci_session = requests.Session()
        oci_session.headers = {"Authorization": f'Bearer {j["token"]}'}
        self.session_map[package] = oci_session
        return oci_session

    def get_blob(self, package, digest):
        url = f"{self.location}/v2/{package}/blobs/{digest}"
        oci_session = self.oci_auth(self.location, package)
        res = oci_session.get(url)
        return res

    def get_tags(self, package):
        url = f"{self.location}/v2/{package}/tags/list"
        oci_session = self.oci_auth(self.location, package)
        res = oci_session.get(url)

        return res.json()["tags"]

    def get_manifest(self, package, tag):
        url = f"{self.location}/v2/{package}/manifests/{tag}"

        oci_session = self.oci_auth(package)
        headers = {"accept": "application/vnd.oci.image.manifest.v1+json"}
        r = oci_session.get(url, headers=headers)

        return r.json()
