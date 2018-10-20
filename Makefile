up:
	docker-compose up

mycli:
	@mycli -h 127.0.0.1 -u root -p secret

test:
	go test `glide novendor -dir message`