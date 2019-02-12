set -e -x

if [ ! -d themes/kube ]; then
    mkdir -p themes/kube && pushd themes/kube
    git init
    git remote add origin https://github.com/jeblister/kube
    git fetch --depth 1 origin 0e5397b788dce3f428aeced1cd30aa309927a2c5
    git checkout FETCH_HEAD
    popd
fi

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
    "message/infrastructure/amqp/doc.go"
    "message/infrastructure/amqp/publisher.go"
    "message/infrastructure/amqp/subscriber.go"
    "message/infrastructure/amqp/config.go"
    "message/infrastructure/amqp/marshaler.go"
    "message/infrastructure/http/publisher.go"
    "message/message.go"
    "message/pubsub.go"
    "message/router.go"
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
