up:

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

test_codecov:
	go test -coverprofile=coverage.out -covermode=atomic ./...

test_reconnect:
	go test -tags=reconnect ./...

build:
	go build ./...

wait:

fmt:
	go fmt ./...
	goimports -l -w .

generate_gomod:
	rm go.mod go.sum || true
	go mod init github.com/ThreeDotsLabs/watermill

	go install ./...
	sed -i '\|go |d' go.mod
	go mod edit -fmt

update_examples_deps:
	go run dev/update-examples-deps/main.go

validate_examples:
	(cd dev/validate-examples/ && go run main.go)
