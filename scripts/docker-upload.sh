#!/bin/sh
# Copyright 2026 Hyperbyte Cloud
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Manually build and push a multi-arch (linux/amd64 + linux/arm64)
# hyperbytedb image to GHCR. CI does the same thing automatically on every
# `v*` tag push (see .github/workflows/release.yml). Use this script when
# you need to ship an image without cutting a tag, e.g. a hotfix RC.
#
# Requirements:
#   - docker with buildx (Docker Desktop or `docker buildx install`)
#   - QEMU registered for cross-arch (`docker run --privileged --rm \
#     tonistiigi/binfmt --install arm64`) — only needed once per host.
#   - ~/.github/cr-pat.txt containing a GHCR PAT with write:packages.

set -e

CR_PAT=$(cat ~/.github/cr-pat.txt)
if [ -z "$CR_PAT" ]; then
    echo "Could not find CR_PAT in ~/.github/cr-pat.txt"
    exit 1
fi

docker login ghcr.io -u austin-barrington -p "$CR_PAT"
registry="ghcr.io/hyperbyte-cloud"
platforms="linux/amd64,linux/arm64"

# Extract version from Cargo.toml (no python/perl, pure POSIX shell)
version=$(
    awk -F' *= *' '
        /^\[package\]/ {intable=1}
        intable && $1=="version" {
            gsub(/"/,"",$2); print $2; exit
        }
    ' Cargo.toml
)

if [ -z "$version" ]; then
    echo "Could not determine version from Cargo.toml"
    exit 1
fi

image="$registry/hyperbytedb:$version"
latest="$registry/hyperbytedb:latest"

# Use a dedicated builder so we don't clobber the default `docker` driver
# (which can't do multi-arch). `--bootstrap` makes the buildkitd container
# come up eagerly so failures surface here rather than mid-build.
if ! docker buildx inspect hyperbytedb-builder >/dev/null 2>&1; then
    docker buildx create --name hyperbytedb-builder --driver docker-container --bootstrap
fi
docker buildx use hyperbytedb-builder

echo "Building and pushing multi-arch image: $image ($platforms)"
docker buildx build \
    --platform "$platforms" \
    --tag "$image" \
    --tag "$latest" \
    --push \
    .
