import pathlib

from conda_oci_mirror.util import sha256sum


class Layer:
    def __init__(self, file, media_type, annotations=None):
        self.file = file
        self.media_type = media_type
        self.annotations = annotations

    def to_dict(self):
        size = pathlib.Path(self.file).stat().st_size
        digest = f"sha256:{sha256sum(self.file)}"

        d = {"mediaType": self.media_type, "size": size, "digest": digest}

        if self.annotations:
            d["annotations"] = self.annotations

        return d
