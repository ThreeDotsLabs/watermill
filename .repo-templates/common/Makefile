up:
	docker-compose up

test:
	go test -parallel 20 ./...

test_v:
	go test -parallel 20 -v ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -parallel 30 -timeout=15m ./...

test_reconnect:
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .

{{ if index . "MakefileCustom" -}}
    {{ .MakefileCustom }}
{{ end }}