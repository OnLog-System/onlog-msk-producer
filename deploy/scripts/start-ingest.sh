#!/usr/bin/env bash
set -euo pipefail

BASE_DIR=/opt/onlog
JAR=$BASE_DIR/ingest-consumer.jar
ENV_FILE=$BASE_DIR/env/consumer.env

set -a
source $ENV_FILE
set +a

exec java $JAVA_OPTS -jar $JAR
