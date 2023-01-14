import pathlib
import subprocess


class ORAS:
    def __init__(self, base_dir="."):
        self.exec = "oras"
        self.base_dir = pathlib.Path(base_dir)

    def run(self, args):
        res = subprocess.run(
            [self.exec] + args, cwd=self.base_dir, stdout=subprocess.DEVNULL
        )
        if res.returncode != 0:
            raise RuntimeError("ORAS had an error (" + " ".join(args) + ")")

    def pull(self, location, subdir, package_name, media_type):
        name, version, build = package_name.rsplit("-", 2)
        location = f"{location}/{subdir}/{name}:{version}-{build}"
        args = ["pull", location, "--media-type", media_type]
        return self.run(args)

    def pull_tag(self, location, subdir, name, tag, media_type):
        location = f"{location}/{subdir}/{name}:{tag}"
        args = ["pull", location, "--media-type", media_type]
        return self.run(args)

    def push(self, target, tag, layers, config=None):
        layer_opts = [f"{str(layer.file)}:{layer.media_type}" for layer in layers]
        dest = f"{target}:{tag}"
        args = ["push", dest] + layer_opts
        return self.run(args)
