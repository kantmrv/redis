#!/bin/sh

set -e
cmake . >/dev/null
make >/dev/null
exec ./server "$@"
