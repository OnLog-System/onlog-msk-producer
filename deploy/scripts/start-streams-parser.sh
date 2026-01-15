#!/usr/bin/env bash
set -euo pipefail

BASE_DIR=/opt/onlog
JAR=$BASE_DIR/streams-parser.jar
ENV_FILE=$BASE_DIR/env/streams.env

set -a
source $ENV_FILE
set +a

exec java $JAVA_OPTS -jar $JAR
