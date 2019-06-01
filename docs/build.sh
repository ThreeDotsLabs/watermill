#!/bin/bash
set -e -x

cd "$(dirname "$0")"

if [[ ! -d themes/kube ]]; then
    mkdir -p themes/kube && pushd themes/kube
    git init
    git remote add origin https://github.com/jeblister/kube
    git fetch --depth 1 origin bda578df413e441fb24e4f5f751d2b15b7efb53a
    git checkout FETCH_HEAD
    popd
fi


if [[ "$1" == "--copy" ]]; then
    rm content/src-link -r || true
    mkdir content/src-link/
    cp ../message/ content/src-link/ -r
    cp ../_examples/ content/src-link/ -r
    cp ../components/ content/src-link/ -r
else
    declare -a files_to_link=(
        "message/infrastructure/kafka/publisher.go"
        "message/infrastructure/kafka/subscriber.go"
        "message/infrastructure/kafka/marshaler.go"
        "message/infrastructure/kafka/config.go"
        "message/infrastructure/nats/publisher.go"
        "message/infrastructure/nats/subscriber.go"
        "message/infrastructure/nats/marshaler.go"
        "message/infrastructure/googlecloud/publisher.go"
        "message/infrastructure/googlecloud/subscriber.go"
        "message/infrastructure/googlecloud/marshaler.go"
        "message/infrastructure/gochannel/pubsub.go"
        "message/infrastructure/http/subscriber.go"
        "message/infrastructure/http/publisher.go"
        "message/infrastructure/amqp/doc.go"
        "message/infrastructure/amqp/publisher.go"
        "message/infrastructure/amqp/subscriber.go"
        "message/infrastructure/amqp/config.go"
        "message/infrastructure/amqp/marshaler.go"
        "message/infrastructure/amqp/topology_builder.go"
        "message/infrastructure/io/publisher.go"
        "message/infrastructure/io/subscriber.go"
        "message/infrastructure/io/marshal.go"
        "message/decorator.go"
        "message/message.go"
        "message/pubsub.go"
        "message/router.go"
        
        "_examples/cqrs-protobuf/main.go"
        "components/cqrs/command_bus.go"
        "components/cqrs/command_processor.go"
        "components/cqrs/event_bus.go"
        "components/cqrs/event_processor.go"
        "components/cqrs/marshaler.go"
        "components/cqrs/cqrs.go"
        "components/cqrs/marshaler.go"

        "components/metrics/builder.go"
        "components/metrics/http.go"
        "_examples/metrics/main.go"
    )

    pushd ../

    for i in "${files_to_link[@]}"
    do
        DIR=$(dirname "${i}")
        DEST_DIR="docs/content/src-link/${DIR}"

        mkdir -p "${DEST_DIR}"
        ln -rsf "./${i}" "./${DEST_DIR}"
    done

    popd
fi

python3 ./extract_middleware_godocs.py > content/src-link/middleware-defs.md

hugo --gc --minify
