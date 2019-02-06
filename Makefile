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

test_stress:
	go test -tags=stress ./...

test_reconnect:
	go test -tags=reconnect ./...
