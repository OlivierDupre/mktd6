#!/usr/bin/env bash

case "$1" in
deps)
    ./mvnw -Dmaven.repo.local=.mvn/m2/repository dependency:sources
;;
install)
    ./mvnw -Dmaven.repo.local=.mvn/m2/repository clean install -DskipTests=true
;;
site)
    ./mvnw -Dmaven.repo.local=.mvn/m2/repository clean install site -DskipTests=true
;;
*)
  echo 'build [deps | install | site]'
;;
esac

