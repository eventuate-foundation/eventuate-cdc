#!/bin/bash -e

export DATABASE=mysql

./scripts/_build-and-test-mysql.sh build
