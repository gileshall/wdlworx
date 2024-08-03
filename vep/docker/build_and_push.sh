#!/bin/bash

set -ex

# used to download image and data cache
VEP_VERSION="release_112.0"
VEP_GENOME_ORGANISM="homo_sapiens"
VEP_GENOME_VERSION="GRCh38"

# used to tag the image
VEP_VERSION_TAG="r112"
VEP_GENOME_ORGANISM_TAG="human"
VEP_GENOME_VERSION_TAG="hg38"

uname_info="$(uname -s)"
case "${uname_info}" in
    Linux*)     machine=linux;;
    Darwin*)    machine=osx;;
    *)          machine="UNKNOWN:${uname_info}"
esac

VEP_DATA_VERSION_TAG="${VEP_VERSION_TAG}_${VEP_GENOME_ORGANISM_TAG}_${VEP_GENOME_VERSION_TAG}"
PUSH_TAG="us-docker.pkg.dev/broad-dsde-methods/exomiser/vep:${VEP_DATA_VERSION_TAG}"

if test "$machine" = "linux"; then
    docker build \
        --build-arg "VEP_VERSION=${VEP_VERSION}" \
        --build-arg "VEP_GENOME_ORGANISM=${VEP_GENOME_ORGANISM}" \
        --build-arg "VEP_GENOME_VERSION=${VEP_GENOME_VERSION}" \
        -t "${PUSH_TAG}" --push .
elif test "$machine" = "osx"; then
    docker buildx build \
        --build-arg "VEP_VERSION=${VEP_VERSION}" \
        --build-arg "VEP_GENOME_ORGANISM=${VEP_GENOME_ORGANISM}" \
        --build-arg "VEP_GENOME_VERSION=${VEP_GENOME_VERSION}" \
        -t "${PUSH_TAG}" --push .
fi
