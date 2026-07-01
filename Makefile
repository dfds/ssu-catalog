BINARY_NAME=ssu-catalog

build:
	go build -o bin/$(BINARY_NAME) ./cmd/main.go

run:
	go run ./cmd/main.go

test:
	go test ./...

test-race:
	go test -race ./...

test-coverage:
	go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

docker-build:
	docker build -t dfdsdk/$(BINARY_NAME):latest .

verify: fmt tidy test-race

clean:
	rm -rf bin/ coverage.out coverage.html

.PHONY: build run test test-race test-coverage lint fmt tidy docker-build verify clean
