import datetime
import logging
import os

import oras as oraslib
import oras.defaults
import oras.oci
import oras.provider
from oras.decorator import ensure_container

logger = logging.getLogger(__name__)


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
        logger.info(f"⭐️ Pushing {uri}: {self.created_at}")

        # The context should be the file root
        with oras.utils.workdir(self.root):
            oras.push(uri, self.layers)


def pull_to_dir(pull_dir, target):
    """
    Given a URI, pull to an output directory.
    """
    reg = get_oras_client()
    return reg.pull(target=target, outdir=pull_dir)


class Registry(oras.provider.Registry):
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
            logger.info(f"Uploading {blob} to {container.uri}")
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
