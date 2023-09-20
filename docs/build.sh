#!/bin/bash
set -e -x

cd "$(dirname "$0")"

if [[ ! -d themes/kube ]]; then
    mkdir -p themes/kube && pushd themes/kube
    git init
    git remote add origin https://github.com/jeblister/kube
    git fetch --depth 1 origin 1507abea527aecd896fdc306dfd28ee3e34f01ec
    git checkout FETCH_HEAD
    popd
fi

function cloneOrPull() {
    if [[ -d "$2" ]]
    then
        pushd $2
        git pull
        popd
    else
        git clone --single-branch $1 $2
    fi
}

if [[ "$1" == "--copy" ]]; then
    rm content/src-link -r || true
    mkdir content/src-link/
    cp ../message/ content/src-link/ -r
    cp ../pubsub/ content/src-link/ -r
    cp ../_examples/ content/src-link/ -r
    cp ../components/ content/src-link/ -r
else
    declare -a files_to_link=(
        "_examples"

        "message/decorator.go"
        "message/message.go"
        "message/pubsub.go"
        "message/router.go"
        "message/router_context.go"
        "pubsub/gochannel/pubsub.go"

        "components/cqrs/command_bus.go"
        "components/cqrs/command_processor.go"
        "components/cqrs/command_handler.go"

        "components/cqrs/event_bus.go"
        "components/cqrs/event_processor.go"
        "components/cqrs/event_processor_group.go"
        "components/cqrs/event_handler.go"

        "components/cqrs/marshaler.go"
        "components/cqrs/cqrs.go"
        "components/cqrs/marshaler.go"

        "components/metrics/builder.go"
        "components/metrics/http.go"

        "components/fanin/fanin.go"
    )

    pushd ../
    for i in "${files_to_link[@]}"
    do
        DIR=$(dirname "${i}")
        DEST_DIR="docs/content/src-link/${DIR}"

        mkdir -p "${DEST_DIR}"
        ln -sf "$PWD/${i}" "$PWD/${DEST_DIR}"
    done
    popd
fi

cloneOrPull "https://github.com/ThreeDotsLabs/watermill-amqp.git" content/src-link/watermill-amqp
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-googlecloud.git" content/src-link/watermill-googlecloud
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-http.git" content/src-link/watermill-http
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-io.git" content/src-link/watermill-io
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-kafka.git" content/src-link/watermill-kafka
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-nats.git" content/src-link/watermill-nats
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-sql.git" content/src-link/watermill-sql
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-firestore.git" content/src-link/watermill-firestore
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-bolt.git" content/src-link/watermill-bolt
cloneOrPull "https://github.com/ThreeDotsLabs/watermill-redisstream.git" content/src-link/watermill-redisstream


python3 ./extract_middleware_godocs.py > content/src-link/middleware-defs.md

hugo --gc --minify
