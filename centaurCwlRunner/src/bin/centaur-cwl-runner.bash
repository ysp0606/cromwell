#!/usr/bin/env bash

# `sbt assembly` must have already been run.
CENTAUR_CWL_JAR="${CENTAUR_CWL_JAR:-"$( find "$( dirname "${BASH_SOURCE[0]}" )/../../target/scala-2.12" -name 'centaur-cwl-runner-*.jar' )"}"

#java -jar "${CENTAUR_CWL_JAR}" $@
java -jar "/Users/danb/cromwell/centaurCwlRunner/target/scala-2.12/centaur-cwl-runner-31-d32d789-SNAP.jar" $@
