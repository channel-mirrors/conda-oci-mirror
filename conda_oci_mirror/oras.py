import subprocess
import pathlib

class ORAS:
    def __init__(self, base_dir="."):
        self.exec = 'oras'
        self.base_dir = pathlib.Path(base_dir)

    def run(self, args):
        res = subprocess.run([self.exec] + args, cwd=self.base_dir)
        if res.returncode != 0:
            raise RuntimeError("ORAS had an error")

    def pull(self, location, subdir, package_name, media_type):
        name, version, build = package_name.rsplit('-', 2)
        location = f'{location}/{subdir}/{name}:{version}-{build}'
        args = ['pull', location, '--media-type', media_type]

        self.run(args)

    def push(self, target, tag, layers, config=None):
        layer_opts = [f'{str(l.file)}:{l.media_type}' for l in layers]
        dest = f'{target}:{tag}'
        args = ["push", dest] + layer_opts

        print("ARGS: ", args)
        return self.run(args)

class Layer:
    def __init__(self, file, media_type):
        self.file = file
        self.media_type = media_type