#!/bin/bash

# A helper script to easily run a development, local registry
docker run -it --rm -p 5000:5000 ghcr.io/oras-project/registry:latest
