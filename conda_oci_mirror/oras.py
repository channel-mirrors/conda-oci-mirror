import datetime
import os
import tempfile
from urllib.parse import urlsplit

import jsonschema
import oras as oraslib
import oras.defaults
import oras.oci
import oras.provider
import oras.schemas
import requests
from oras.decorator import ensure_container

from conda_oci_mirror import defaults, util
from conda_oci_mirror.logger import logger


def registry_name(registry):
    """Strip transport from an OCI reference prefix, retaining its namespace."""
    return registry.split("://", 1)[-1].rstrip("/") if registry else registry


# The OCI image-spec 1.1 empty descriptor, the config of every manifest the mirror writes.
# oras-py's default config is a zero-byte blob, which some registries reject (Amazon ECR), so a
# pull-through cache in front of the mirror could never store a package.
EMPTY_CONFIG = {
    "mediaType": "application/vnd.oci.empty.v1+json",
    "size": 2,
    "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    "data": "e30=",
}
EMPTY_CONFIG_CONTENT = b"{}"
# The spec requires an artifactType next to the empty descriptor. This is the oras CLI's default.
ARTIFACT_TYPE = "application/vnd.unknown.artifact.v1"
# oras-py 0.1.14 rejects manifest keys it does not know, artifactType among them.
MANIFEST_SCHEMA = {
    **oraslib.schemas.manifest,
    "properties": {
        **oraslib.schemas.manifest["properties"],
        "artifactType": {"type": "string"},
    },
}


def get_oras_client(registry=None, insecure=False):
    """Create an independent client with explicit transport and current credentials."""
    target = registry or ""
    parsed = urlsplit(target if "://" in target else "https://" + target)
    if any(
        (
            parsed.scheme not in ("http", "https"),
            parsed.username is not None,
            parsed.password is not None,
            parsed.query,
            parsed.fragment,
        )
    ):
        raise ValueError(
            "Registry must be an HTTP(S) host/namespace without URL credentials, query or fragment"
        )
    user = os.environ.get("ORAS_USER")
    password = os.environ.get("ORAS_PASS")
    reg = Registry(
        hostname=parsed.netloc or None, insecure=insecure or parsed.scheme == "http"
    )
    if user and password:
        logger.info("Found username and password for basic auth")
        reg.set_basic_auth(user, password)
        reg.has_auth = True
    else:
        logger.warning("ORAS_USER or ORAS_PASS is missing, push may have issues.")
        reg.has_auth = False
    return reg


class Pusher:
    """
    Class to handle layers and pushing with oras
    """

    def __init__(self, root, timestamp=None, client=None, preserve_existing=True):
        self.preserve_existing = preserve_existing
        self.client = client
        self.root = root
        self.layers = []
        self.timestamp = timestamp or datetime.datetime.now().astimezone()

    @property
    def created_at(self):
        """
        Get the created at time.
        """
        # A string timestamp was already provided
        if isinstance(self.timestamp, str):
            return self.timestamp
        return self.timestamp.strftime("%Y.%m.%d.%H.%M")

    def add_layer(self, path, media_type, title=None, annotations=None):
        """
        Helper function to add a layer.

        If a title is not provided, we assume the same as the relative path.
        """
        if not title:
            title = path

        # If the path doesn't exist, assume in the root
        if not os.path.exists(path):
            path = os.path.join(self.root, path)
        if not os.path.exists(path):
            raise FileExistsError(f"{path} does not exist.")

        annotations = annotations or {}
        annotations.update({"creationTime": self.created_at})
        self.layers.append(
            {
                "path": os.path.abspath(path),
                "title": title,
                "media_type": media_type,
                "annotations": annotations,
            }
        )

    def push(self, uri):
        """
        uri is the registry name with tag.
        """
        # Add some custom annotations!
        logger.debug(f"⭐️ Pushing {uri}: {self.created_at}")

        client = self.client or get_oras_client(uri)
        client.push(uri, self.layers, preserve_existing=self.preserve_existing)

        # Return lookup with URI and layers
        return {"uri": uri, "layers": self.layers}


class Registry(oras.provider.Registry):
    @property
    def session(self):
        # Each worker owns its connection pool, even when created with fork.
        if self._session is None or self._session_pid != os.getpid():
            if self._session is not None:
                self._session.close()
            self.session = requests.Session()
        return self._session

    @session.setter
    def session(self, session):
        self._session = session
        self._session_pid = os.getpid()

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_session"] = None
        state["_session_pid"] = None
        return state

    def _check_200_response(self, response):
        response.raise_for_status()
        return super()._check_200_response(response)

    @ensure_container
    def get_manifest(self, container, allowed_media_type=None):
        """oras-py's get_manifest, validating against a schema that knows artifactType."""
        accept = allowed_media_type or [oraslib.defaults.default_manifest_media_type]
        response = self.do_request(
            f"{self.prefix}://{container.get_manifest_url()}",
            "GET",
            headers={"Accept": ";".join(accept)},
        )
        self._check_200_response(response)
        manifest = response.json()
        jsonschema.validate(manifest, schema=MANIFEST_SCHEMA)
        return manifest

    def upload_manifest(self, manifest, container):
        """oras-py's upload_manifest, validating against a schema that knows artifactType."""
        self.reset_basic_auth()
        jsonschema.validate(manifest, schema=MANIFEST_SCHEMA)
        return self.do_request(
            f"{self.prefix}://{container.put_manifest_url()}",
            "PUT",
            headers={"Content-Type": oraslib.defaults.default_manifest_media_type},
            json=manifest,
        )

    @ensure_container
    def get_optional_manifest(self, container):
        try:
            return self.get_manifest(container)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def set_insecure(self):
        """
        Change the prefix used (http/https) based on user preference.
        """
        self.prefix = "http"

    def download_blob(self, container, digest, outfile):
        """Verify blobs before replacing files, for both regular and media-type pulls."""
        parent = os.path.dirname(os.path.abspath(outfile))
        os.makedirs(parent, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as staging_dir:
            staged = os.path.join(staging_dir, "download")
            super().download_blob(container, digest, staged)
            if digest != f"sha256:{util.sha256sum(staged)}":
                raise ValueError(f"Checksum mismatch downloading {digest}")
            os.replace(staged, outfile)
        return outfile

    @ensure_container
    def pull_by_media_type(self, container, dest, media_type=None):
        """
        Given a manifest of layers, retrieve a layer based on desired media type
        """
        dest = os.path.abspath(dest)
        # Tags (including latest) can move between pulls.
        manifest = self.get_manifest(container)

        # Let's return a list of download paths to the user
        paths = []

        # Find the layer of interest! Currently we look for presence of the string
        # e.g., "prices" can come from "prices" or "prices-web"
        for layer in manifest.get("layers", []):
            # E.g., google.prices or google.prices-web or aws.prices
            if media_type and layer["mediaType"] != media_type:
                continue

            # This annotation is currently the practice for a relative path to extract to
            artifact = layer["annotations"]["org.opencontainers.image.title"]

            # This raises an error if there is a malicious path
            outfile = oraslib.utils.sanitize_path(dest, os.path.join(dest, artifact))

            # If it already exists with the same digest, don't do it :)
            if os.path.exists(outfile):
                expected_digest = f"sha256:{util.sha256sum(outfile)}"
                if layer["digest"] == expected_digest:
                    print(
                        f"{outfile} already exists with expected hash, not re-downloading."
                    )
                    paths.append(outfile)
                    continue

            print(f"Downloading {artifact} to {outfile}")
            path = self.download_blob(container, layer["digest"], outfile)
            paths.append(path)

        return paths

    @ensure_container
    def push(self, container, archives: list, preserve_existing=True):
        """
        Given a dict of layers (paths and corresponding mediaType) push.
        """

        # Prepare a new manifest
        manifest = oraslib.oci.NewManifest()

        # Upload files as blobs
        for item in archives:
            blob = item.get("path")
            media_type = item.get("media_type")
            annots = item.get("annotations") or {}

            if not blob or not os.path.exists(blob):
                logger.warning(f"Path {blob} does not exist or is not defined.")
                continue

            # Artifact title is basename or user defined
            blob_name = item.get("title") or os.path.basename(blob)

            # If it's a directory, we need to compress
            cleanup_blob = False
            if os.path.isdir(blob):
                blob = oraslib.utils.make_targz(blob)
                cleanup_blob = True

            # Create a new layer from the blob
            layer = oraslib.oci.NewLayer(blob, media_type, is_dir=cleanup_blob)
            logger.debug(f"Preparing layer {layer}")

            # Update annotations with title we will need for extraction
            annots.update({oraslib.defaults.annotation_title: blob_name})
            layer["annotations"] = annots

            # update the manifest with the new layer
            manifest["layers"].append(layer)

            # Upload the blob layer
            logger.info(f"Uploading {blob_name} to {container.uri}")
            response = self.upload_blob(blob, container, layer)
            self._check_200_response(response)

            # Do we need to cleanup a temporary targz?
            if cleanup_blob and os.path.exists(blob):
                os.remove(blob)

        # A package tag may contain both archive formats. Preserve the other one
        # by reusing its descriptor; no blob download or re-upload is needed.
        package_types = {
            defaults.package_conda_media_type,
            defaults.package_tarbz2_media_type,
        }
        uploaded_types = {layer["mediaType"] for layer in manifest["layers"]}
        if preserve_existing and uploaded_types & package_types:
            previous = self.get_optional_manifest(container)
            if previous:
                keep = package_types - uploaded_types
                manifest["layers"].extend(
                    layer
                    for layer in previous.get("layers", [])
                    if layer["mediaType"] in keep
                )

        # The config is just another blob; the manifest carries the empty descriptor.
        with tempfile.NamedTemporaryFile(suffix=".json") as config:
            config.write(EMPTY_CONFIG_CONTENT)
            config.flush()
            self._check_200_response(
                self.upload_blob(config.name, container, EMPTY_CONFIG)
            )

        # Final upload of the manifest
        manifest["config"] = EMPTY_CONFIG
        manifest["artifactType"] = ARTIFACT_TYPE
        response = self.upload_manifest(manifest, container)
        self._check_200_response(response)
        print(f"Successfully pushed {container}")
        return response

    @ensure_container
    def push_single_layer(self, container, path, media_type, title):
        """
        Push a one-layer artifact with three requests: blob POST, blob PUT, manifest PUT.

        push() authenticates every request from scratch and uploads the config
        each time, roughly 11 requests per artifact. For many small
        artifacts in one repository (repodata shards), reuse the bearer token
        and upload the config once.
        """
        layer = oraslib.oci.NewLayer(path, media_type)
        layer["annotations"] = {oraslib.defaults.annotation_title: title}
        self._ensure_config_blob(container)
        with open(path, "rb") as blob:
            self._upload_blob_with_token(container, blob.read(), layer["digest"])

        manifest = oraslib.oci.NewManifest()
        manifest["layers"].append(layer)
        manifest["config"] = EMPTY_CONFIG
        manifest["artifactType"] = ARTIFACT_TYPE
        response = self._request_with_token(
            container.upload_blob_url(),
            "PUT",
            f"{self.prefix}://{container.put_manifest_url()}",
            headers={"Content-Type": oraslib.defaults.default_manifest_media_type},
            json=manifest,
        )
        self._check_200_response(response)
        return response

    def _ensure_config_blob(self, container):
        """Upload the empty config blob once per repository."""
        repository = container.upload_blob_url()
        configs = self.__dict__.setdefault("_configs_pushed", set())
        if repository not in configs:
            self._upload_blob_with_token(
                container, EMPTY_CONFIG_CONTENT, EMPTY_CONFIG["digest"]
            )
            configs.add(repository)

    def _upload_blob_with_token(self, container, data, digest):
        repository = container.upload_blob_url()
        response = self._request_with_token(
            repository,
            "POST",
            f"{self.prefix}://{repository}",
            headers={"Content-Type": "application/octet-stream"},
        )
        self._check_200_response(response)
        session_url = self._get_location(response, container)
        if not session_url:
            raise ValueError(f"Issue retrieving session url for {container}")
        response = self._request_with_token(
            repository,
            "PUT",
            oraslib.utils.append_url_params(session_url, {"digest": digest}),
            headers={
                "Content-Length": str(len(data)),
                "Content-Type": "application/octet-stream",
            },
            data=data,
        )
        self._check_200_response(response)
        return response

    def _request_with_token(self, repository, method, url, headers=None, **kwargs):
        """Send a request with this repository's bearer token, renewing it on 401."""
        tokens = self.__dict__.setdefault("_repository_tokens", {})
        headers = dict(headers or {})
        if repository in tokens:
            headers["Authorization"] = tokens[repository]
        response = self.session.request(method, url, headers=headers, **kwargs)
        if response.status_code == 401:
            # Log in with the credentials, never with a stale bearer token. The
            # token is kept out of self.headers so other requests are unaffected.
            self.reset_basic_auth()
            if self.authenticate_request(response):
                tokens[repository] = headers["Authorization"] = self.headers[
                    "Authorization"
                ]
                response = self.session.request(method, url, headers=headers, **kwargs)
            self.reset_basic_auth()
        return response


# Backward-compatible standalone client; mirror operations never use or mutate it.
oras = get_oras_client()
