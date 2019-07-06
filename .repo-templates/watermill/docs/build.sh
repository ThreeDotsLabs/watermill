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

{{ range .Config.Repositories -}}{{ $repoConfig := . }}
{{- range .Templates -}}
{{- if eq (.) "pubsub" -}}
cloneOrPull {{ $repoConfig.URL }} content/src-link/{{ $repoConfig.Name }}
{{ end }}
{{- end -}}
{{- end }}

declare -a files_to_link=(
    "message/decorator.go"
    "message/message.go"
    "message/pubsub.go"
    "message/router.go"
    "message/infrastructure/gochannel/pubsub.go"

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

python3 ./extract_middleware_godocs.py > content/src-link/middleware-defs.md

hugo --gc --minify
