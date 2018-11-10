up:
	docker-compose up

mycli:
	@mycli -h 127.0.0.1 -u root -p secret

test:
	go test `glide novendor`

test_stress:
	go test -tags=stress `glide novendor`