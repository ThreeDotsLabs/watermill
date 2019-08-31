#!/bin/bash
set -e

readonly pubsub="$1"

if [ -z "$pubsub" ]; then
    echo "Usage: $0 <pubsub>"
    exit 1
fi

docker-compose -f "./compose/$pubsub.yml" up -d

sleep 20

docker-compose -f "./compose/$pubsub.yml" -f ./compose/watermill.yml run \
    -v "$(pwd):/benchmark" \
    -w /benchmark \
    watermill go run main.go -pubsub "$pubsub"
