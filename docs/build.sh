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

git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-amqp.git content/src-link/watermill-amqp
git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-googlecloud.git content/src-link/watermill-googlecloud
git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-http.git content/src-link/watermill-http
git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-io.git content/src-link/watermill-io
git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-kafka.git content/src-link/watermill-kafka
git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-nats.git content/src-link/watermill-nats
git clone --single-branch --branch master git@github.com:ThreeDotsLabs/watermill-sql.git content/src-link/watermill-sql



declare -a files_to_link=(
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

hugo --gc --minify
