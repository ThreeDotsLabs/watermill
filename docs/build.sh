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

function cloneOrPull() {
    if [[ -d "$2" ]]
    then
        pushd $2
        git pull
        popd
    else
        git clone --single-branch --branch master $1 $2
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
        "pubsub/gochannel/pubsub.go"

        "components/cqrs/command_bus.go"
        "components/cqrs/command_processor.go"
        "components/cqrs/event_bus.go"
        "components/cqrs/event_processor.go"
        "components/cqrs/marshaler.go"
        "components/cqrs/cqrs.go"
        "components/cqrs/marshaler.go"

        "components/metrics/builder.go"
        "components/metrics/http.go"
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


python3 ./extract_middleware_godocs.py > content/src-link/middleware-defs.md

hugo --gc --minify
