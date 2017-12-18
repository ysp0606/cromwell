#!/usr/bin/env bash

# `sbt assembly` must have already been run.
CENTAUR_CWL_JAR="${CENTAUR_CWL_JAR:-"$( find "$( dirname "${BASH_SOURCE[0]}" )/../../target/scala-2.12" -name 'centaur-cwl-runner-*.jar' )"}"
SERVICE_ACCOUNT_JSON="$( dirname "${BASH_SOURCE[0]}" )/../../../cromwell-service-account.json"

if [ -e "${SERVICE_ACCOUNT_JSON}" ]
then
  MODE="--mode-gcs-service=${SERVICE_ACCOUNT_JSON}"
else
  MODE="--mode-local"
fi

# Allow setting "CENTAUR_CWL_RUNNER_MODE=--mode-gcs-default" for local GCS Application Default usage.
CENTAUR_CWL_RUNNER_MODE=${CENTAUR_CWL_RUNNER_MODE:-$MODE}

>&2 echo using CENTAUR_CWL_RUNNER_MODE="${CENTAUR_CWL_RUNNER_MODE}"

java -jar "${CENTAUR_CWL_JAR}" "${CENTAUR_CWL_RUNNER_MODE}" "$@"
