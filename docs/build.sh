set -e -x

if [ ! -d themes/kube ]; then
    mkdir -p themes/kube && pushd themes/kube
    git init
    git remote add origin https://github.com/jeblister/kube
    git fetch --depth 1 origin 5f68bf3e990eff4108fa251f3a3112d081fffba4
    git checkout FETCH_HEAD
    popd
fi

pushd ../
mkdir -p docs/content/src-link/message/infrastructure/kafka
mkdir -p docs/content/src-link/message/infrastructure/gochannel

ln -rsf message/infrastructure/kafka/subscriber.go docs/content/src-link/message/infrastructure/kafka/
ln -rsf message/infrastructure/gochannel/pubsub.go docs/content/src-link/message/infrastructure/gochannel/
popd

hugo --gc --minify
