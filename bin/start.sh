#!/usr/bin/env bash

./server -config config_aws.json -log_dir=. -log_level=$3 -id=$1.$2 > s1 2>s2_$1_$2.err &
