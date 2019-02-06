#!/bin/bash
set -e

readonly WAIT_FOR_IT_URL=https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh
readonly TARGET=/tmp/wait-for-it.sh

if [ ! -f "$TARGET" ]; then
    wget "$WAIT_FOR_IT_URL" -O "$TARGET"
    chmod +x "$TARGET"
fi

for service in zookeeper:2181 rabbitmq:5672 googlecloud:8085 nats-streaming:8222 kafka:9092; do
    "$TARGET" -t 60 "$service"
done
