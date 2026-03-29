#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <path-to-frontend>" >&2
    exit 1
fi

frontend_dir="$1"
static_dir="$(cd "$(dirname "$0")/.." && pwd)/underfit_api/static"

cd "$frontend_dir"
npm ci
npm run build
rm -rf "$static_dir"
cp -r dist "$static_dir"
