#!/usr/bin/env bash

#-memprof=/home/aleksey/fleetmem.mprof

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    rm logs/*
    #echo "Process id for servers is written to location: {$PID_FILE}"
    ./server -log_dir=logs -id 1.1 &
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 1.2 &
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 2.1 &
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 2.2 &
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 3.1 &
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 3.2 &
    #echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
