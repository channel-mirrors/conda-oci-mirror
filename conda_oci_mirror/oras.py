import datetime
import os

import oras as oraslib
import oras.defaults
import oras.oci
import oras.provider
from oras.decorator import ensure_container

import conda_oci_mirror.util as util
from conda_oci_mirror.logger import logger


def get_oras_client():
    """
    Consistent method to get an oras client
    """
    user = os.environ.get("ORAS_USER")
    password = os.environ.get("ORAS_PASS")
    reg = Registry()
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

    def __init__(self, root, timestamp=None):
        self.root = root
        self.layers = []
        self.timestamp = timestamp or datetime.datetime.now()

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
        size = os.path.getsize(path)  # bytes
        annotations = {"creationTime": self.created_at, "size": str(size)}
        self.layers.append(
            {
                "path": path,
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

        # The context should be the file root
        with oraslib.utils.workdir(self.root):
            oras.push(uri, self.layers)

        # Return lookup with URI and layers
        return {"uri": uri, "layers": self.layers}


# Cache of manifests
manifest_cache = {}


class Registry(oras.provider.Registry):
    def set_insecure(self):
        """
        Change the prefix used (http/https) based on user preference.
        """
        self.prefix = "http"

    @ensure_container
    def pull_by_media_type(self, container, dest, media_type=None):
        """
        Given a manifest of layers, retrieve a layer based on desired media type
        """
        # Keep a cache of manifests
        global manifest_cache
        if container.uri not in manifest_cache:
            manifest_cache[container.uri] = self.get_manifest(container)
        manifest = manifest_cache[container.uri]

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

            # this function  handles creating the output directory if does not exist
            print(f"Downloading {artifact} to {outfile}")
            path = self.download_blob(container, layer["digest"], outfile)
            paths.append(path)

        return paths

    @ensure_container
    def push(self, container, archives: list):
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

        # Prepare manifest and config
        # Note that we don't add annotations, etc. here
        conf, config_file = oraslib.oci.ManifestConfig()

        # Config is just another layer blob!
        response = self.upload_blob(config_file, container, conf)
        self._check_200_response(response)

        # Final upload of the manifest
        manifest["config"] = conf
        self._check_200_response(self.upload_manifest(manifest, container))
        print(f"Successfully pushed {container}")
        return response


# Create global oras client to manage mirrors
oras = get_oras_client()
