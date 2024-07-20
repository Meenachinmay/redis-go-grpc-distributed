run:
	docker-compose down && docker-compose up --build -d
stop:
	docker-compose down
cc:
	go run client/main.go
