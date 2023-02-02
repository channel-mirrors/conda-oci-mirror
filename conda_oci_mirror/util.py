import errno
import hashlib
import json
import os
import platform
import subprocess
import sys
import tarfile


def print_item(prefix, item):
    """
    Shared function to print an item (prefix) and pretty item.
    """
    if isinstance(
        item,
        (
            list,
            tuple,
        ),
    ):
        item = " ".join(item)
    print(f"{prefix} {item}")


def write_file(text, filename):
    """
    Write some text to a filename
    """
    with open(filename, "w") as fo:
        fo.write(text)


def write_json(obj, filename):
    """
    Write json to filename.
    """
    with open(filename, "w") as fd:
        fd.write(json.dumps(obj, indent=4))
    return filename


def read_json(filename):
    """
    Read json file into dict/list etc.
    """
    with open(filename) as fi:
        content = json.load(fi)
    return content


def mkdir_p(path):
    """
    Make a directory path if it does not exist, akin to mkdir -p
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            sys.exit(f"Error creating path {path}, exiting.")


def compress_folder(source_dir, output_filename):
    """
    Compress a directory to an output destination
    """
    if not platform.system() == "Windows":
        return subprocess.run(
            f"tar -cvzf {output_filename} *",
            cwd=source_dir,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    else:
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=".")


def sha256sum(path):
    hash_func = hashlib.sha256()

    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)

    return hash_func.hexdigest()


def md5sum(fn):
    curr_sha = hashlib.md5()
    with open(fn, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            curr_sha.update(byte_block)

    return curr_sha.hexdigest()
