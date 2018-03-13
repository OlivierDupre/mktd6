#! /usr/bin/env bash

export LOCAL_IP=$(ifconfig \
    | grep -Eo 'inet (addr:)?192\.([0-9]*\.){2}[0-9]*' \
    | grep -Eo '([0-9]*\.){3}[0-9]*')

echo Local IP: $LOCAL_IP

docker-compose down && docker-compose up
