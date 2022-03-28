import requests

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

    def get_blob(self, package, digest):
        package = self.full_package(package)

        url = f"{self.location}/v2/{package}/blobs/{digest}"
        oci_session = self.oci_auth(self.location, package)
        res = oci_session.get(url)
        return res

    def get_tags(self, package, n_tags=10_000, prev_last=None):
        package = self.full_package(package)

        url = f"{self.location}/v2/{package}/tags/list?n={n_tags}"
        if prev_last:
            url += "&last=prev_last"
        oci_session = self.oci_auth(self.location, package)

        tags = []
        link = True
        # get all tags using the pagination
        while link:
            res = oci_session.get(url)
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
