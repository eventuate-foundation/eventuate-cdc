#!/bin/bash -e

export DATABASE=mysql8

./scripts/_build-and-test-mysql.sh build
