.PHONY: tidy build up down logs

tidy:
	cd go-api && go mod tidy
	cd go-consumer && go mod tidy

build:
	mkdir -p bin
	cd go-api && go build -o ../bin/api .
	cd go-consumer && go build -o ../bin/consumer .

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api consumer python-ai
