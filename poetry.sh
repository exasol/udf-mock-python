#!/bin/bash

set -e
set -u
set -o pipefail

poetry "$@"
echo "Converting pyproject.toml to setup.py"
dephell deps convert
