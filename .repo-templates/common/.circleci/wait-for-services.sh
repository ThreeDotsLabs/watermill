#!/bin/bash
set -e

{{ if (.CircleCIWaitForServices) ne "" -}}
for service in {{ .CircleCIWaitForServices }}; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
{{ else -}}
# no requirements
{{ end -}}
