#!/usr/bin/env bash

memprof=/home/aleksey/fleetmem.mprof

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    rm logs/*
    #echo "Process id for servers is written to location: {$PID_FILE}"
    ./server -log_dir=logs -id 1.1 >logs/s11.txt 2>logs/e11.txt &
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 1.2 >logs/s12.txt 2>logs/e12.txt&
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 2.1 >logs/s21.txt 2>logs/e21.txt&
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 2.2 >logs/s22.txt 2>logs/e22.txt&
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 3.1 >logs/s31.txt 2>logs/e31.txt&
    #echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 3.2 >logs/s32.txt 2>logs/e32.txt&
    #echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
