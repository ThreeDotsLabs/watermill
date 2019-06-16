up:
	docker-compose up

mycli:
	@mycli -h 127.0.0.1 -u root -p secret

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
	go mod init github.com/ThreeDotsLabs/watermill
	go install ./...
	go get -u github.com/golang/protobuf/proto
	sed -i '\|go |d' go.mod
	go mod edit -fmt

{{ .Variables.MakefileCustom }}