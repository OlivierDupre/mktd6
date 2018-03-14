#!/usr/bin/env bash

case "$1" in
server)
    ./mvnw \
        -Dmaven.repo.local=.mvn/m2/repository \
        -pl monkonomy-kafkastreams-serverapp \
        exec:java \
        -Dexec.mainClass=io.monkeypatch.mktd6.server.MonkonomyServer
;;
trader)
    ./mvnw \
        -Dmaven.repo.local=.mvn/m2/repository \
        -pl monkonomy-kafkastreams-trader \
        exec:java \
        -Dexec.mainClass=io.monkeypatch.mktd6.Main
;;
*)
  echo 'run [server]'
;;
esac

