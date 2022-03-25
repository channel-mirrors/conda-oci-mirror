from datetime import datetime
import json
import logging
from operator import index
from pathlib import Path
import pathlib
import subprocess
import urllib.request
from conda_oci_mirror.oci import OCI
from conda_oci_mirror.oras import ORAS, Layer
oci = OCI ("https://ghcr.io", "MichaelKora")
all_sub_dirs = [
    "linux-64",
    "osx-64",
    "osx-arm64",
    "win-64",
    "linux-aarch64",
    "linux-ppc64le",
    "noarch",
]

def get_all_packages(repodata):
#    download_link = f"https://conda.anaconda.org/{channel}/{subdir}/repodata.json"
    found_packages = []

    #download the repodata.json file
#    with urllib.request.urlopen(download_link) as url:
#        repodata = json.loads(url.read().decode())
    for key in repodata["packages"]:
        pkg_name = repodata["packages"][key]["name"]
        found_packages.append(key)
    return found_packages

def compare_checksums(base, all_subdirs):
    
    differences = {
    "linux-64":[],
    "osx-64":[],
    "osx-arm64":[],
    "win-64":[],
    "linux-aarch64":[],
    "linux-ppc64le":[],
    "noarch":[]
    }
    for subdir in all_subdirs:
        repodata_path = Path(base) / subdir / "repodata.json"
    
        with open(repodata_path) as fi:
            repodata = json.load(fi)

        found_packages = get_all_packages(repodata)
        
        #test( to be deleted b4 run on production)
        found_packages = ["zlib","xtensor-blas"]

        for pkg_name in found_packages:
            full_name = "conda-forge/" + subdir + "/"+ pkg_name
            tags = oci.get_tags(full_name)

            for tag in tags:
                key = pkg_name+"-"+tag+".tar.bz2"   
                print ("key: " + key)

                sha_repodata = "sha256:"+repodata["packages"][key]["sha256"]
                print ("sha_repodata: " + sha_repodata)
                
                manifest = oci.get_manifest( full_name, tag)
                sha_manifest = ""

                for layer in manifest["layers"]:
                    if layer["mediaType"] == "application/vnd.conda.package.v1":
                        sha_manifest = layer["digest"]
                        print ("sha_manifest: " + sha_manifest)
                        if sha_repodata != sha_manifest:
                            differences[subdir].append(key)
    
    return differences


def upload_index_json(global_index,channel,remote_loc):


    for key in global_index:
        #itterate throughevery pkg. e.g: zlib
        subdir = global_index ["info"]["subdir"]
        index_file = {"info": {"subdir":{} }}
        index_file ["info"]["subdir"]=subdir
            
        if key != "info":
            index_file["name"]=key
            
            #go through all the versions of a specific package. eg: zlib-12.0-1. zlib-12.0-2 
            for pkg in global_index[key]:
                pkg_name = pkg["name"] + "-" + pkg["version"] +  "-" + pkg["build"]
                index_file[pkg_name] = pkg
            
            dir_index = pathlib.Path(channel) / subdir / key
            dir_index.mkdir(mode=511, parents=True, exist_ok=True)
            
            json_object = json.dumps(index_file, indent = 4) 

            index_path = dir_index / "index.json"

            with open(index_path, "w") as write_file:
                json.dump(json_object, write_file)
                
            logging.warning(f"upload the index.json file...")
            upload_path = channel + "/" + subdir + "/" + index_file["name"] + "/index.json" 
            
            #upload_index_json(str(dir_index), remote_loc)
            now = datetime.now()
            tag = now.strftime("%d%m%Y%H%M%S")
            oras = ORAS(base_dir=dir_index)
            media_type = "application/json"
            #layers = [Layer(upload_path, media_type)]
            layers = [Layer("index.json", media_type)]
            logging.warning(f"upload the json file for <<{key}>>")
             #target, tag, layers
            oras.push(
                f"{remote_loc}/{upload_path}", tag, layers
            )

            oras.push(
                f"{remote_loc}/{upload_path}", "latest", layers
            )

            #upload_cmd = f"oras push {remote_loc}/{upload_path}:latest {upload_path}:application/json"
            #upload_cmd_2 = f"oras push {remote_loc}/{upload_path}:{tag} {upload_path}:application/json"
            #subprocess.run(upload_cmd, shell=True)
            #subprocess.run(upload_cmd_2, shell=True)
            logging.warning(f"index.json successfuly uploaded for {key}!")
            print(json_object)
            #subprocess.run(f"cat {index_path}",shell=True)
            

        


