up:
	docker-compose up

test:
	go test ./...

test_v:
	go test -v ./...

test_short:
	go test ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -timeout=30m ./...

test_reconnect:
	go test -tags=reconnect ./...

validate_examples:
	go run dev/update-examples-deps/main.go
	bash dev/validate_examples.sh

generate_gomod:
	rm go.mod go.sum || true
	go mod init {{ .GoPackage }}
{{ if ne .Name "watermill" -}}
	# todo - change to last release
	go get github.com/ThreeDotsLabs/watermill@master
{{ end }}
	go install ./...
	sed -i '\|go |d' go.mod
	go mod edit -fmt

{{ if index . "MakefileCustom" }}
    {{ .MakefileCustom }}
{{ end }}